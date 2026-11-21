// Shopping Sessions pipeline: rules.md in one Rust program.
// read (mmap) -> links union-find -> events by event_id hash: dedup + validate -> by person hash:
// sort, sessions, credit, assists -> two Parquet tables.
// Speed: every string key is hashed once to 64 bits; every hash match is confirmed on the real bytes,
// and a bucket that ever sees two different strings with one hash is redone with exact string keys.
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::collections::HashMap;
use std::fs;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};

mod pq;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY: i64 = 86_400_000;
const GAP: u32 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const NB: usize = 256; // hash buckets
const CHUNK: usize = 4 << 20; // bytes per parse task

pub type B = &'static [u8];

// identity hasher for keys that already are good 64-bit hashes
#[derive(Default)]
struct NoHash(u64);
impl Hasher for NoHash {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n
    }
}
type U64Map<V> = HashMap<u64, V, BuildHasherDefault<NoHash>>;

#[inline]
fn hash(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(b);
    let x = h.finish();
    // finalizer so every bit range is well mixed (buckets use bits 40..48, the map uses low and top bits)
    let x = (x ^ (x >> 33)).wrapping_mul(0xff51afd7ed558ccd);
    x ^ (x >> 29)
}

/// A second, independent hash: checks that equal first hashes really are equal strings.
#[inline]
fn hash2(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write_u64(0x9e37_79b9_7f4a_7c15);
    h.write(b);
    let x = h.finish().wrapping_add(b.len() as u64);
    let x = (x ^ (x >> 31)).wrapping_mul(0xbf58476d1ce4e5b9);
    x ^ (x >> 32)
}

#[inline]
fn bucket(h: u64) -> usize {
    (h >> 40) as usize & (NB - 1)
}

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

/// Bodies of CSV files (after the header line), split into chunks that end on line boundaries.
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

#[inline(always)]
fn unquote(f: B, quotes: u32) -> Option<B> {
    if quotes == 0 {
        Some(f)
    } else if quotes == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
        Some(&f[1..f.len() - 1])
    } else {
        None
    }
}

/// Split a line into exactly N fields following the CSV dialect; None if malformed.
#[inline(always)]
fn fields<const N: usize>(line: B) -> Option<[B; N]> {
    // 8 bytes at a time: flag bytes that may be ',' or '"' (the zero-byte trick can also flag a byte
    // right above a real match, so every flagged byte is re-checked below; real matches are never missed)
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    let mut out: [B; N] = [&[]; N];
    let mut n = 0;
    let mut s = 0;
    let mut q = 0u32;
    let len = line.len();
    let mut i = 0;
    macro_rules! byte {
        ($pos:expr) => {{
            let pos = $pos;
            let c = line[pos];
            if c == b',' {
                if n + 1 >= N {
                    return None;
                }
                out[n] = unquote(&line[s..pos], q)?;
                n += 1;
                s = pos + 1;
                q = 0;
            } else if c == b'"' {
                q += 1;
            }
        }};
    }
    while i + 8 <= len {
        let w = u64::from_le_bytes(line[i..i + 8].try_into().unwrap());
        let a = w ^ (ONES * b',' as u64);
        let b = w ^ (ONES * b'"' as u64);
        let mut m = ((a.wrapping_sub(ONES) & !a) | (b.wrapping_sub(ONES) & !b)) & HIGH;
        while m != 0 {
            byte!(i + (m.trailing_zeros() / 8) as usize);
            m &= m - 1;
        }
        i += 8;
    }
    while i < len {
        byte!(i);
        i += 1;
    }
    if n + 1 != N {
        return None;
    }
    out[n] = unquote(&line[s..], q)?;
    Some(out)
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
    let mut v: i64 = 0; // F3: digits only
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

// ---------- records ----------

/// A parsed event line, before dedup. `line` lets us re-read the id and user only when needed.
#[derive(Clone, Copy)]
struct Ev {
    idh: u64,
    ph: u64,
    ph2: u32,
    id: B,
    name: B,
    ingest: i64,
    ts: u32, // ms since base (window_start - 7 days)
    price: u32,
    cat: u16,
    ok: bool,
}

/// A surviving event, keyed by person.
#[derive(Clone, Copy)]
struct PEv {
    ph: u64,
    ph2: u32,
    name: B,
    ts: u32,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct PCl {
    ph: u64,
    ph2: u32,
    name: B,
    id: B,
    ts: u32,
    camp: u16,
}

/// What a lookup of a linked id needs, in one table slot: a second hash of the id (the check),
/// and the person's name with its two hashes.
#[derive(Clone, Copy)]
struct LinkVal {
    h2: u64,
    nh: u64,
    nh2: u32,
    name: B,
}

enum Links {
    Fast(U64Map<LinkVal>),
    Exact(FxHashMap<B, LinkVal>),
}

impl Links {
    /// (person hash, second person hash, person name) for a user id whose hash is `h`.
    #[inline]
    fn person(&self, user: B, h: u64) -> (u64, u32, B) {
        let h2 = hash2(user);
        let v = match self {
            Links::Fast(m) => m.get(&h).filter(|v| v.h2 == h2),
            Links::Exact(m) => m.get(user),
        };
        match v {
            Some(v) => (v.nh, v.nh2, v.name),
            None => (h, h2 as u32, user),
        }
    }
}

struct Ctx {
    base: i64,
    ws: i64,
    we: i64,
    products: FxHashMap<B, u16>,
    unknown_cat: u16,
    camps: FxHashMap<B, u16>,
    links: Links,
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

#[inline]
fn parse_event(line: B, cx: &Ctx) -> Option<Ev> {
    let [id, user, ts, ingest, typ, product, price] = fields::<7>(line)?;
    if id.is_empty() {
        return None;
    }
    let ingest = parse_ts(ingest).unwrap_or(i64::MAX);
    let mut e = Ev { idh: hash(id), ph: 0, ph2: 0, id, name: &[], ingest, ts: 0, price: 0, cat: 0, ok: false };
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
    e.ts = (t - cx.base) as u32;
    e.ok = true;
    (e.ph, e.ph2, e.name) = cx.links.person(user, hash(user));
    Some(e)
}

#[inline]
fn parse_click(line: B, cx: &Ctx) -> Option<PCl> {
    let [id, user, camp, ts] = fields::<4>(line)?;
    if id.is_empty() || user.is_empty() {
        return None;
    }
    let t = parse_ts(ts)?;
    if t < cx.base || t >= cx.we {
        return None;
    }
    let c = *cx.camps.get(camp)?;
    let (ph, ph2, name) = cx.links.person(user, hash(user));
    Some(PCl { ph, ph2, name, id, ts: (t - cx.base) as u32, camp: c })
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

fn build_links(chunks: &[B], force_exact: bool) -> Links {
    let pairs: Vec<Vec<(B, B)>> = chunks
        .par_iter()
        .map(|c| {
            let mut v = Vec::with_capacity(c.len() / 20);
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
    // union-find over interned ids (exact string keys; links are a small share of the data)
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut idx: FxHashMap<B, u32> = FxHashMap::with_capacity_and_hasher(total, Default::default());
    let mut ids: Vec<B> = Vec::with_capacity(total);
    let mut par: Vec<u32> = Vec::with_capacity(total);
    for v in &pairs {
        for &(a, b) in v {
            let mut get = |s: B| {
                *idx.entry(s).or_insert_with(|| {
                    ids.push(s);
                    par.push(par.len() as u32);
                    (ids.len() - 1) as u32
                })
            };
            let (ia, ib) = (get(a), get(b));
            let (ra, rb) = (find(&mut par, ia), find(&mut par, ib));
            if ra != rb {
                par[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    drop(idx);
    let n = ids.len();
    let mut best: Vec<B> = ids.clone();
    for i in 0..n {
        let r = find(&mut par, i as u32) as usize;
        if ids[i] < best[r] {
            best[r] = ids[i];
        }
    }
    let mut root: Vec<(u64, u32)> = vec![(0, 0); n];
    for i in 0..n {
        if par[i] as usize == i {
            root[i] = (hash(best[i]), hash2(best[i]) as u32);
        }
    }
    let val = |i: usize| {
        let r = par[i] as usize;
        LinkVal { h2: hash2(ids[i]), nh: root[r].0, nh2: root[r].1, name: best[r] }
    };
    if !force_exact {
        let mut m: U64Map<LinkVal> = U64Map::with_capacity_and_hasher(n, Default::default());
        let mut ok = true;
        for i in 0..n {
            if m.insert(hash(ids[i]), val(i)).is_some() {
                ok = false; // two link ids share a 64-bit hash
                break;
            }
        }
        if ok {
            return Links::Fast(m);
        }
        eprintln!("links: hash collision, exact mode");
    }
    let mut m: FxHashMap<B, LinkVal> = FxHashMap::with_capacity_and_hasher(n, Default::default());
    for i in 0..n {
        m.insert(ids[i], val(i));
    }
    Links::Exact(m)
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

// ---------- dedup ----------

/// Keep, per event id, the copy with the smallest ingest_ts; return the kept copies that are valid.
fn dedup(recs: &[&Ev], exact: bool) -> Option<Vec<usize>> {
    let mut keep = Vec::with_capacity(recs.len());
    if exact {
        let mut m: FxHashMap<B, usize> = FxHashMap::default();
        for (k, e) in recs.iter().enumerate() {
            m.entry(e.id).and_modify(|j| {
                if e.ingest < recs[*j].ingest {
                    *j = k
                }
            })
            .or_insert(k);
        }
        keep.extend(m.values().copied().filter(|&k| recs[k].ok));
        return Some(keep);
    }
    let mut m: U64Map<u32> = U64Map::with_capacity_and_hasher(recs.len(), Default::default());
    for (k, e) in recs.iter().enumerate() {
        match m.entry(e.idh) {
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(k as u32);
            }
            std::collections::hash_map::Entry::Occupied(mut o) => {
                let j = *o.get() as usize;
                if e.id != recs[j].id {
                    return None; // hash collision between two different ids
                }
                if e.ingest < recs[j].ingest {
                    o.insert(k as u32);
                }
            }
        }
    }
    keep.extend(m.values().map(|&k| k as usize).filter(|&k| recs[k].ok));
    Some(keep)
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
    // scratch
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

struct Shared<'a> {
    ncamp: usize,
    day0: i64, // first window day, in days since epoch
    base: i64,
    cat_rank: &'a [u32],
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

/// One person: events sorted by ts, clicks sorted by (ts asc, id desc).
fn process_person(acc: &mut Acc, sh: &Shared, name: B, evs: &[PEv], cls: &[PCl]) {
    let nd = acc.nd;
    let week = WEEK as u32;
    let (mut ci, mut cj) = (0usize, 0usize); // click window [ci, cj)
    let mut cur: Option<DayAgg> = None;
    let mut i = 0;
    while i < evs.len() {
        let start = evs[i].ts;
        let mut last = start;
        let mut j = i;
        let mut rev: u64 = 0;
        while j < evs.len() && evs[j].ts - last <= GAP {
            last = evs[j].ts;
            j += 1;
        }
        let day = (sh.base + start as i64).div_euclid(DAY);
        if cur.as_ref().map(|c| c.day) != Some(day) {
            if let Some(c) = cur.take() {
                flush_day(acc, name, &c, sh.cat_rank);
            }
            cur = Some(DayAgg { day, sessions: 0, events: 0, rev: 0 });
        }
        for e in &evs[i..j] {
            if e.price > 0 {
                rev += e.price as u64;
                let p = e.price as u64;
                match acc.catsum.iter_mut().find(|x| x.0 == e.cat) {
                    Some(x) => x.1 += p,
                    None => acc.catsum.push((e.cat, p)),
                }
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
        // start >= week always (start is in the window, base is 7 days earlier)
        while ci < cj && cls[ci].ts < start - week {
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
        let d = (day - sh.day0) as usize;
        let credited = if cj > ci { cls[cj - 1].camp as usize } else { sh.ncamp };
        let cell = credited * nd + d;
        acc.sess[cell] += 1;
        acc.rev[cell] += rev;
        if rev > 0 {
            acc.purch[cell] += 1;
        }
        for k in 0..acc.active.len() {
            acc.assist[acc.active[k] as usize * nd + d] += 1;
        }
        i = j;
    }
    if let Some(c) = cur.take() {
        flush_day(acc, name, &c, sh.cat_rank);
    }
    for k in 0..acc.active.len() {
        acc.cnt[acc.active[k] as usize] = 0;
    }
    acc.active.clear();
}

/// All persons of one bucket. Fast mode groups by person hash and checks the names;
/// returns false (nothing written) if two names share a hash, and exact mode is used instead.
fn process_bucket(acc: &mut Acc, sh: &Shared, evs: &mut [PEv], cls: &mut [PCl], exact: bool) -> bool {
    if exact {
        evs.sort_unstable_by(|a, b| (a.ph, a.name, a.ts).cmp(&(b.ph, b.name, b.ts)));
        cls.sort_unstable_by(|a, b| (a.ph, a.name, a.ts).cmp(&(b.ph, b.name, b.ts)).then_with(|| b.id.cmp(a.id)));
    } else {
        evs.sort_unstable_by_key(|e| (e.ph, e.ts));
        cls.sort_unstable_by(|a, b| (a.ph, a.ts).cmp(&(b.ph, b.ts)).then_with(|| b.id.cmp(a.id)));
        // verify: one name per hash
        for w in evs.windows(2) {
            if w[0].ph == w[1].ph && w[0].ph2 != w[1].ph2 {
                return false;
            }
        }
        let mut i = 0;
        for c in cls.iter() {
            while i < evs.len() && evs[i].ph < c.ph {
                i += 1;
            }
            if i < evs.len() && evs[i].ph == c.ph && evs[i].ph2 != c.ph2 {
                return false;
            }
        }
    }
    let same = |a: (u64, B), b: (u64, B)| a.0 == b.0 && (!exact || a.1 == b.1);
    let (mut i, mut k) = (0usize, 0usize);
    while i < evs.len() {
        let key = (evs[i].ph, evs[i].name);
        let mut j = i + 1;
        while j < evs.len() && same((evs[j].ph, evs[j].name), key) {
            j += 1;
        }
        // skip clicks of persons with no surviving events (same order as the sort)
        while k < cls.len() && (if exact { (cls[k].ph, cls[k].name) < key } else { cls[k].ph < key.0 }) {
            k += 1;
        }
        let mut l = k;
        while l < cls.len() && same((cls[l].ph, cls[l].name), key) {
            l += 1;
        }
        process_person(acc, sh, key.1, &evs[i..j], &cls[k..l]);
        k = l;
        i = j;
    }
    true
}

fn main() {
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();
    let t0 = std::time::Instant::now();
    let timing = std::env::var("SS_TIME").is_ok();
    let force_exact = std::env::var("SS_EXACT").is_ok(); // test switch: always use exact string keys
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

    let links = build_links(&ln_chunks, force_exact);
    lap("links");
    let base = ws - WEEK;
    let cx = Ctx { base, ws, we, products, unknown_cat, camps, links };

    // events: parse into buckets by event_id hash; clicks: into buckets by person hash
    let est = |c: &B, line: usize| c.len() / line / NB + 16;
    let ev_parts: Vec<Vec<Vec<Ev>>> = ev_chunks
        .par_iter()
        .map(|c| {
            let mut bk: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::with_capacity(est(c, 60))).collect();
            for_lines(c, |l| {
                if let Some(e) = parse_event(l, &cx) {
                    bk[bucket(e.idh)].push(e);
                }
            });
            bk
        })
        .collect();
    lap("events parsed");
    let cl_parts: Vec<Vec<Vec<PCl>>> = cl_chunks
        .par_iter()
        .map(|c| {
            let mut bk: Vec<Vec<PCl>> = (0..NB).map(|_| Vec::with_capacity(est(c, 40))).collect();
            for_lines(c, |l| {
                if let Some(k) = parse_click(l, &cx) {
                    bk[bucket(k.ph)].push(k);
                }
            });
            bk
        })
        .collect();
    lap("clicks parsed");

    // dedup events per id bucket, route the valid kept copies to person buckets
    let pe_parts: Vec<Vec<Vec<PEv>>> = (0..NB)
        .into_par_iter()
        .map(|b| {
            let n: usize = ev_parts.iter().map(|p| p[b].len()).sum();
            let mut recs: Vec<&Ev> = Vec::with_capacity(n);
            for p in &ev_parts {
                recs.extend(p[b].iter());
            }
            let keep = match dedup(&recs, force_exact) {
                Some(k) => k,
                None => {
                    eprintln!("dedup: hash collision in bucket {b}, exact mode");
                    dedup(&recs, true).unwrap()
                }
            };
            let mut bk: Vec<Vec<PEv>> = (0..NB).map(|_| Vec::with_capacity(keep.len() / NB + 16)).collect();
            for k in keep {
                let e = &recs[k];
                bk[bucket(e.ph)].push(PEv { ph: e.ph, ph2: e.ph2, name: e.name, ts: e.ts, price: e.price, cat: e.cat });
            }
            bk
        })
        .collect();
    drop(ev_parts);
    lap("events deduped");

    let day0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let sh = Shared { ncamp, day0, base, cat_rank: &cat_rank };
    let accs: Vec<Acc> = (0..NB)
        .into_par_iter()
        .fold(
            || Acc::new(ncamp, nd),
            |mut acc, b| {
                let mut evs: Vec<PEv> = Vec::with_capacity(pe_parts.iter().map(|p| p[b].len()).sum());
                for p in &pe_parts {
                    evs.extend_from_slice(&p[b]);
                }
                let mut cls: Vec<PCl> = Vec::with_capacity(cl_parts.iter().map(|p| p[b].len()).sum());
                for p in &cl_parts {
                    cls.extend_from_slice(&p[b]);
                }
                if !process_bucket(&mut acc, &sh, &mut evs, &mut cls, force_exact) {
                    eprintln!("persons: hash collision in bucket {b}, exact mode");
                    process_bucket(&mut acc, &sh, &mut evs, &mut cls, true);
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
