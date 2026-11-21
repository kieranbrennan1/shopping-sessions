// Shopping Sessions pipeline: rules.md implemented in one pass-oriented Rust program.
// Stages: read inputs (mmap) -> links union-find -> events (dedup by id, validate) and clicks
// -> group by person -> sessions, credit, assists -> two Parquet tables.
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::fs;
use std::hash::Hasher;
use std::path::{Path, PathBuf};

mod pq;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const NB: usize = 256; // hash buckets
const CHUNK: usize = 2 << 20; // bytes per parse task

type B = &'static [u8];

// ---------- input files ----------

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let n = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                n.ends_with(".csv") && !n.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> B {
    let f = fs::File::open(p).expect("open");
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f).expect("mmap") };
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

/// Body of a CSV file (after the header line), split into chunks that end on line boundaries.
fn chunks_of(files: &[PathBuf]) -> Vec<B> {
    let mut out = Vec::new();
    for p in files {
        let data = map_file(p);
        let body = match memchr::memchr(b'\n', data) {
            Some(i) => &data[i + 1..],
            None => continue, // header only (or nothing)
        };
        let mut s = 0;
        while s < body.len() {
            let mut e = (s + CHUNK).min(body.len());
            if e < body.len() {
                e = match memchr::memchr(b'\n', &body[e..]) {
                    Some(i) => e + i + 1,
                    None => body.len(),
                };
            }
            out.push(&body[s..e]);
            s = e;
        }
    }
    out
}

/// Iterate the lines of a chunk ("\n" ends a line; a final line without "\n" still counts).
#[inline]
fn for_lines(chunk: B, mut f: impl FnMut(B)) {
    let mut s = 0;
    for i in memchr::memchr_iter(b'\n', chunk) {
        f(&chunk[s..i]);
        s = i + 1;
    }
    if s < chunk.len() {
        f(&chunk[s..]);
    }
}

/// Split a line into exactly N fields following the CSV dialect; None if malformed.
#[inline]
fn fields<const N: usize>(line: B) -> Option<[B; N]> {
    let mut out: [B; N] = [&[]; N];
    let mut n = 0;
    let mut s = 0;
    let len = line.len();
    loop {
        let e = match memchr::memchr(b',', &line[s..]) {
            Some(i) => s + i,
            None => len,
        };
        if n == N {
            return None;
        }
        let f = &line[s..e];
        out[n] = if f.first() == Some(&b'"') {
            if f.len() < 2 || f[f.len() - 1] != b'"' || memchr::memchr(b'"', &f[1..f.len() - 1]).is_some() {
                return None;
            }
            &f[1..f.len() - 1]
        } else {
            if memchr::memchr(b'"', f).is_some() {
                return None;
            }
            f
        };
        n += 1;
        if e == len {
            break;
        }
        s = e + 1;
    }
    if n == N { Some(out) } else { None }
}

// ---------- timestamps ----------

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
    if a < 10 && c < 10 { Some((a * 10 + c) as i64) } else { None }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Parse F1 / F2 / F3 into epoch ms. None = bad timestamp (window checked by caller).
#[inline]
fn parse_ts(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if n == 0 {
        return None;
    }
    if (n == 20 || n == 25) && f[4] == b'-' {
        return parse_iso(f);
    }
    // F3: digits only
    let mut v: i64 = 0;
    for &c in f {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as i64)?;
    }
    Some(v)
}

fn parse_iso(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if f[7] != b'-' || f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let y = d2(f, 0)? * 100 + d2(f, 2)?;
    let mo = d2(f, 5)?;
    let d = d2(f, 8)?;
    let h = d2(f, 11)?;
    let mi = d2(f, 14)?;
    let s = d2(f, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let dim = match mo {
        2 => {
            if leap(y) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let mut t = (days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + s) * 1000;
    if n == 20 {
        if f[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match f[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if f[22] != b':' {
            return None;
        }
        let oh = d2(f, 20)?;
        let om = d2(f, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        t -= sign * (oh * 3600 + om * 60) * 1000;
    }
    Some(t)
}

#[inline]
fn hash(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(b);
    h.finish()
}

#[inline]
fn bucket(h: u64) -> usize {
    (h >> 56) as usize % NB
}

// ---------- records ----------

#[derive(Clone, Copy)]
struct Ev {
    id: B,
    user: B,
    ts: i64,
    ingest: i64,
    price: u32, // 0 unless a valid purchase
    cat: u16,
    ok: bool,
}

#[derive(Clone, Copy)]
struct PEv {
    name: B,
    ts: i64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct PCl {
    name: B,
    id: B,
    ts: i64,
    camp: u16,
}

struct Ctx {
    ws: i64,
    we: i64,
    products: FxHashMap<B, u16>,
    unknown_cat: u16,
    camps: FxHashMap<B, u16>,
    link_idx: FxHashMap<B, u32>,
    link_name: Vec<B>,
}

impl Ctx {
    #[inline]
    fn person(&self, user: B) -> B {
        match self.link_idx.get(user) {
            Some(&i) => self.link_name[i as usize],
            None => user,
        }
    }
}

fn parse_price(p: &[u8]) -> Option<u32> {
    if p.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in p {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None; // leading zeros keep v small, so only a real overflow lands here
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

fn parse_event(line: B, cx: &Ctx) -> Option<Ev> {
    let [id, user, ts, ingest, typ, product, price] = fields::<7>(line)?;
    if id.is_empty() {
        return None;
    }
    let ingest = parse_ts(ingest).unwrap_or(i64::MAX);
    let mut e = Ev { id, user, ts: 0, ingest, price: 0, cat: 0, ok: false };
    if user.is_empty() || product.is_empty() {
        return Some(e);
    }
    let t = match parse_ts(ts) {
        Some(t) if t >= cx.ws && t < cx.we => t,
        _ => return Some(e),
    };
    match typ {
        b"view" | b"cart" => {}
        b"purchase" => match parse_price(price) {
            Some(p) => {
                e.price = p;
                e.cat = *cx.products.get(product).unwrap_or(&cx.unknown_cat);
            }
            None => return Some(e),
        },
        _ => return Some(e),
    }
    if ingest - t > DAY {
        return Some(e);
    }
    e.ts = t;
    e.ok = true;
    Some(e)
}

fn parse_click(line: B, cx: &Ctx) -> Option<PCl> {
    let [id, user, camp, ts] = fields::<4>(line)?;
    if id.is_empty() || user.is_empty() {
        return None;
    }
    let t = parse_ts(ts)?;
    if t < cx.ws - WEEK || t >= cx.we {
        return None;
    }
    let c = *cx.camps.get(camp)?;
    Some(PCl { name: cx.person(user), id, ts: t, camp: c })
}

// ---------- links ----------

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let g = p[p[x as usize] as usize];
        p[x as usize] = g;
        x = g;
    }
    x
}

fn build_links(chunks: &[B]) -> (FxHashMap<B, u32>, Vec<B>) {
    let pairs: Vec<Vec<(B, B)>> = chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            for_lines(c, |l| {
                if let Some([a, b]) = fields::<2>(l) {
                    if !a.is_empty() && !b.is_empty() && a != b {
                        v.push((a, b));
                    }
                }
            });
            v
        })
        .collect();
    let mut idx: FxHashMap<B, u32> = FxHashMap::default();
    let mut ids: Vec<B> = Vec::new();
    let mut par: Vec<u32> = Vec::new();
    let intern = |s: B, idx: &mut FxHashMap<B, u32>, ids: &mut Vec<B>, par: &mut Vec<u32>| -> u32 {
        *idx.entry(s).or_insert_with(|| {
            ids.push(s);
            par.push(par.len() as u32);
            (ids.len() - 1) as u32
        })
    };
    for v in &pairs {
        for &(a, b) in v {
            let ia = intern(a, &mut idx, &mut ids, &mut par);
            let ib = intern(b, &mut idx, &mut ids, &mut par);
            let (ra, rb) = (find(&mut par, ia), find(&mut par, ib));
            if ra != rb {
                par[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    let n = ids.len();
    let mut best: Vec<B> = ids.clone();
    for i in 0..n {
        let r = find(&mut par, i as u32) as usize;
        if ids[i] < best[r] {
            best[r] = ids[i];
        }
    }
    let names: Vec<B> = (0..n).map(|i| best[par[i] as usize]).collect();
    (idx, names)
}

// ---------- small inputs ----------

fn read_small<const N: usize>(p: &Path) -> Vec<[B; N]> {
    let mut v = Vec::new();
    for c in chunks_of(&[p.to_path_buf()]) {
        for_lines(c, |l| {
            if let Some(f) = fields::<N>(l) {
                v.push(f);
            }
        });
    }
    v
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let i = text.find(&format!("\"{key}\"")).expect("manifest key") + key.len() + 2;
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    parse_ts(rest[a..b].as_bytes()).expect("manifest ts")
}

// ---------- per-person work ----------

pub struct Acc {
    nd: usize,
    // campaign_daily cells, index c * nd + d; c = ncamp is organic
    sess: Vec<u64>,
    purch: Vec<u64>,
    rev: Vec<u64>,
    assist: Vec<u64>,
    // user_daily rows
    pub u_name: Vec<B>,
    pub u_day: Vec<i32>,
    pub u_sess: Vec<i64>,
    pub u_events: Vec<i64>,
    pub u_rev: Vec<i64>,
    pub u_top: Vec<Option<u16>>,
    // scratch for assists and top category
    cnt: Vec<u32>,
    active: Vec<u16>,
    pos: Vec<u32>,
    catsum: Vec<(u16, u64)>,
}

impl Acc {
    fn new(ncamp: usize, nd: usize) -> Acc {
        let cells = (ncamp + 1) * nd;
        Acc {
            nd,
            sess: vec![0; cells],
            purch: vec![0; cells],
            rev: vec![0; cells],
            assist: vec![0; cells],
            u_name: vec![],
            u_day: vec![],
            u_sess: vec![],
            u_events: vec![],
            u_rev: vec![],
            u_top: vec![],
            cnt: vec![0; ncamp],
            active: vec![],
            pos: vec![0; ncamp],
            catsum: vec![],
        }
    }
}

struct DayAgg {
    day: i64,
    sessions: i64,
    events: i64,
    rev: i64,
}

fn flush_day(acc: &mut Acc, name: B, da: &DayAgg, cat_rank: &[u32]) {
    let mut top: Option<(u64, u16)> = None;
    for &(c, s) in &acc.catsum {
        top = match top {
            Some((bs, bc)) if !(s > bs || (s == bs && cat_rank[c as usize] < cat_rank[bc as usize])) => Some((bs, bc)),
            _ => Some((s, c)),
        };
    }
    acc.u_name.push(name);
    acc.u_day.push(da.day as i32);
    acc.u_sess.push(da.sessions);
    acc.u_events.push(da.events);
    acc.u_rev.push(da.rev);
    acc.u_top.push(top.map(|t| t.1));
    acc.catsum.clear();
}

fn add_cat(acc: &mut Acc, c: u16, p: u32) {
    for e in acc.catsum.iter_mut() {
        if e.0 == c {
            e.1 += p as u64;
            return;
        }
    }
    acc.catsum.push((c, p as u64));
}

fn process_person(acc: &mut Acc, name: B, evs: &mut [PEv], cls: &mut [PCl], day0: i64, ncamp: usize, cat_rank: &[u32]) {
    evs.sort_unstable_by_key(|e| e.ts);
    // ts ascending, id descending: the last click with ts <= start is the latest, smallest id
    cls.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| b.id.cmp(a.id)));
    let nd = acc.nd;
    let (mut ci, mut cj) = (0usize, 0usize); // click window [ci, cj)
    let mut cur: Option<DayAgg> = None;
    let mut i = 0;
    while i < evs.len() {
        let start = evs[i].ts;
        let mut last = start;
        let mut j = i;
        while j < evs.len() && evs[j].ts - last <= GAP {
            last = evs[j].ts;
            j += 1;
        }
        let day = start.div_euclid(DAY);
        if cur.as_ref().map(|c| c.day) != Some(day) {
            if let Some(c) = cur.take() {
                flush_day(acc, name, &c, cat_rank);
            }
            cur = Some(DayAgg { day, sessions: 0, events: 0, rev: 0 });
        }
        let mut rev: u64 = 0;
        for e in &evs[i..j] {
            if e.price > 0 {
                rev += e.price as u64;
                add_cat(acc, e.cat, e.price);
            }
        }
        {
            let c = cur.as_mut().unwrap();
            c.sessions += 1;
            c.events += (j - i) as i64;
            c.rev += rev as i64;
        }
        while cj < cls.len() && cls[cj].ts <= start {
            let c = cls[cj].camp as usize;
            if acc.cnt[c] == 0 {
                acc.pos[c] = acc.active.len() as u32;
                acc.active.push(c as u16);
            }
            acc.cnt[c] += 1;
            cj += 1;
        }
        while ci < cj && cls[ci].ts < start - WEEK {
            let c = cls[ci].camp as usize;
            acc.cnt[c] -= 1;
            if acc.cnt[c] == 0 {
                let p = acc.pos[c] as usize;
                let lastc = *acc.active.last().unwrap();
                acc.active[p] = lastc;
                acc.pos[lastc as usize] = p as u32;
                acc.active.pop();
            }
            ci += 1;
        }
        let d = (day - day0) as usize;
        let credited = if cj > ci { cls[cj - 1].camp as usize } else { ncamp };
        let cell = credited * nd + d;
        acc.sess[cell] += 1;
        acc.rev[cell] += rev;
        if rev > 0 {
            acc.purch[cell] += 1;
        }
        for k in 0..acc.active.len() {
            let a = acc.active[k] as usize;
            acc.assist[a * nd + d] += 1;
        }
        i = j;
    }
    if let Some(c) = cur.take() {
        flush_day(acc, name, &c, cat_rank);
    }
    for k in 0..acc.active.len() {
        acc.cnt[acc.active[k] as usize] = 0;
    }
    acc.active.clear();
}

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();
    let t0 = std::time::Instant::now();
    let timing = std::env::var("SS_TIME").is_ok();
    let lap = |s: &str| {
        if timing {
            eprintln!("{:>8.1} ms  {s}", t0.elapsed().as_secs_f64() * 1000.0)
        }
    };

    // small files
    let text = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&text, "window_start");
    let we = manifest_ts(&text, "window_end");
    let mut cat_names: Vec<B> = Vec::new();
    let mut cat_idx: FxHashMap<B, u16> = FxHashMap::default();
    let mut products: FxHashMap<B, u16> = FxHashMap::default();
    for [p, c] in read_small::<2>(&data.join("products.csv")) {
        let ci = *cat_idx.entry(c).or_insert_with(|| {
            cat_names.push(c);
            (cat_names.len() - 1) as u16
        });
        products.insert(p, ci);
    }
    let unknown: B = b"unknown";
    let unknown_cat = *cat_idx.entry(unknown).or_insert_with(|| {
        cat_names.push(unknown);
        (cat_names.len() - 1) as u16
    });
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(cat_names[b]));
    let mut cat_rank = vec![0u32; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u32;
    }
    let mut camp_names: Vec<B> = Vec::new();
    let mut camp_chan: Vec<B> = Vec::new();
    let mut camps: FxHashMap<B, u16> = FxHashMap::default();
    for [c, ch] in read_small::<2>(&data.join("campaigns.csv")) {
        camps.entry(c).or_insert_with(|| {
            camp_names.push(c);
            camp_chan.push(ch);
            (camp_names.len() - 1) as u16
        });
    }
    let ncamp = camp_names.len();

    let ev_chunks = chunks_of(&csv_files(&data.join("events")));
    let cl_chunks = chunks_of(&csv_files(&data.join("clicks")));
    let ln_chunks = chunks_of(&csv_files(&data.join("links")));
    lap("mapped");

    let (link_idx, link_name) = build_links(&ln_chunks);
    lap("links");
    let cx = Ctx { ws, we, products, unknown_cat, camps, link_idx, link_name };

    // events: parse into buckets by event_id
    let ev_parts: Vec<Vec<Vec<Ev>>> = ev_chunks
        .par_iter()
        .map(|c| {
            let mut bk: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::new()).collect();
            for_lines(c, |l| {
                if let Some(e) = parse_event(l, &cx) {
                    bk[bucket(hash(e.id))].push(e);
                }
            });
            bk
        })
        .collect();
    lap("events parsed");
    // clicks: parse into person buckets
    let cl_parts: Vec<Vec<Vec<PCl>>> = cl_chunks
        .par_iter()
        .map(|c| {
            let mut bk: Vec<Vec<PCl>> = (0..NB).map(|_| Vec::new()).collect();
            for_lines(c, |l| {
                if let Some(k) = parse_click(l, &cx) {
                    bk[bucket(hash(k.name))].push(k);
                }
            });
            bk
        })
        .collect();
    lap("clicks parsed");
    // dedup events per id bucket (smallest ingest_ts wins), keep valid ones, route to person buckets
    let pe_parts: Vec<Vec<Vec<PEv>>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            let n: usize = ev_parts.iter().map(|p| p[b].len()).sum();
            let mut best: FxHashMap<B, Ev> = FxHashMap::with_capacity_and_hasher(n, Default::default());
            for p in &ev_parts {
                for &e in &p[b] {
                    best.entry(e.id)
                        .and_modify(|k| {
                            if e.ingest < k.ingest {
                                *k = e
                            }
                        })
                        .or_insert(e);
                }
            }
            let mut bk: Vec<Vec<PEv>> = (0..NB).map(|_| Vec::new()).collect();
            for e in best.values() {
                if e.ok {
                    let name = cx.person(e.user);
                    bk[bucket(hash(name))].push(PEv { name, ts: e.ts, price: e.price, cat: e.cat });
                }
            }
            bk
        })
        .collect();
    drop(ev_parts);
    lap("events deduped");

    let day0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let accs: Vec<Acc> = (0..NB)
        .into_par_iter()
        .fold(
            || Acc::new(ncamp, nd),
            |mut acc, b| {
                let mut pidx: FxHashMap<B, u32> = FxHashMap::default();
                let mut names: Vec<B> = Vec::new();
                let mut evs: Vec<Vec<PEv>> = Vec::new();
                for p in &pe_parts {
                    for &e in &p[b] {
                        let i = *pidx.entry(e.name).or_insert_with(|| {
                            names.push(e.name);
                            evs.push(Vec::new());
                            (names.len() - 1) as u32
                        });
                        evs[i as usize].push(e);
                    }
                }
                let mut cls: Vec<Vec<PCl>> = (0..names.len()).map(|_| Vec::new()).collect();
                for p in &cl_parts {
                    for &k in &p[b] {
                        if let Some(&i) = pidx.get(k.name) {
                            cls[i as usize].push(k);
                        }
                    }
                }
                for i in 0..names.len() {
                    process_person(&mut acc, names[i], &mut evs[i], &mut cls[i], day0, ncamp, &cat_rank);
                }
                acc
            },
        )
        .collect();
    lap("sessions");

    // campaign_daily
    let mut cd = pq::CampaignRows::default();
    for c in 0..=ncamp {
        for d in 0..nd {
            let k = c * nd + d;
            let (mut s, mut p, mut r, mut a) = (0u64, 0u64, 0u64, 0u64);
            for acc in &accs {
                s += acc.sess[k];
                p += acc.purch[k];
                r += acc.rev[k];
                a += acc.assist[k];
            }
            if s > 0 || a > 0 {
                let (id, ch): (B, B) = if c == ncamp { (b"organic", b"organic") } else { (camp_names[c], camp_chan[c]) };
                cd.id.push(id);
                cd.channel.push(ch);
                cd.day.push((day0 + d as i64) as i32);
                cd.sessions.push(s as i64);
                cd.purchasing.push(p as i64);
                cd.revenue.push(r as i64);
                cd.assisted.push(a as i64);
            }
        }
    }
    let ud = out.join("user_daily");
    let cdir = out.join("campaign_daily");
    fs::create_dir_all(&ud).expect("mkdir");
    fs::create_dir_all(&cdir).expect("mkdir");
    let cat_names = &cat_names;
    rayon::scope(|s| {
        s.spawn(|_| pq::write_campaign(&cdir.join("part-0.parquet"), &cd));
        let mut first = true;
        for (i, acc) in accs.iter().enumerate() {
            if acc.u_name.is_empty() && !first {
                continue;
            }
            first = false;
            let ud = &ud;
            s.spawn(move |_| pq::write_user(&ud.join(format!("part-{i}.parquet")), acc, cat_names));
        }
    });
    lap("written");
}
