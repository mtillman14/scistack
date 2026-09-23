// Turns scan.json (from pyscan.js) into the text reports behind
// docs/claude/cleanup-audit.md §1. Usage: node report.js <out_dir>
// Expects <out_dir>/{scan.json,files.txt}; run from the repo root.
const fs = require('fs');
const path = require('path');

const OUT = process.argv[2];
const ROOT = process.env.AUDIT_ROOT || process.cwd();
const o = JSON.parse(fs.readFileSync(path.join(OUT, 'scan.json'), 'utf8'));
const isTest = f => /(^|\/)tests?\//.test(f) || /\.test\./.test(f);
const isNoise = f => f.startsWith('.claude') || isTest(f);
const write = (name, lines) => fs.writeFileSync(path.join(OUT, name), lines.join('\n') + '\n');
const pad = (v, n) => String(v).padStart(n);

const fns = o.functions.filter(f => !isNoise(f.file));

// functions.txt — longest and widest
{
  const L = ['== longest (lines params location name)'];
  [...fns].sort((a, b) => b.len - a.len).slice(0, 40)
    .forEach(f => L.push(`${pad(f.len, 5)} ${pad(f.params, 3)} ${f.file}:${f.line} ${f.name}`));
  L.push('', '== most params (params lines location name)');
  [...fns].sort((a, b) => b.params - a.params).slice(0, 25)
    .forEach(f => L.push(`${pad(f.params, 3)} ${pad(f.len, 5)} ${f.file}:${f.line} ${f.name}`));
  L.push('', `total ${fns.length}`);
  for (const x of [100, 200, 400]) L.push(`>${x} lines: ${fns.filter(f => f.len > x).length}`);
  L.push(`>10 params: ${fns.filter(f => f.params > 10).length}`);
  write('functions.txt', L);
}

// imports.txt — cross-package graph, private imports, lazy imports
{
  const imps = o.imports.filter(i => !isNoise(i.file));
  const edges = {};
  imps.forEach(i => { const k = `${i.own} -> ${i.pkg}`; edges[k] = (edges[k] || 0) + 1; });
  const L = ['== cross-package import edges (count edge)'];
  Object.entries(edges).sort().forEach(([k, n]) => L.push(`${pad(n, 4)} ${k}`));
  L.push('', '== private cross-package imports');
  imps.filter(i => i.priv.length || i.privMod)
    .forEach(i => L.push(`${i.own} -> ${i.mod}: ${i.priv.join(', ') || '(private module)'}  ${i.file}:${i.line}`));
  const lazy = Object.entries(o.lazyImports).filter(([f]) => !isNoise(f));
  const byPkg = {};
  lazy.forEach(([f, n]) => { const k = f.split('/')[0]; byPkg[k] = (byPkg[k] || 0) + n; });
  L.push('', `== lazy (function-level) imports: ${lazy.reduce((s, [, n]) => s + n, 0)}`);
  Object.entries(byPkg).filter(([, n]) => n).sort((a, b) => b[1] - a[1]).forEach(([k, n]) => L.push(`${pad(n, 5)} ${k}`));
  L.push('', '== top files');
  lazy.sort((a, b) => b[1] - a[1]).slice(0, 15).forEach(([f, n]) => L.push(`${pad(n, 5)} ${f}`));
  write('imports.txt', L);
}

// excepts.txt — broad excepts whose body swallows the error
{
  const be = o.broadExcept.filter(b => !isNoise(b.file));
  const L = [`broad except: ${be.length}, swallowing: ${be.filter(b => b.swallow).length}`, ''];
  be.filter(b => b.swallow).forEach(b => L.push(`${b.file}:${b.line}`));
  write('excepts.txt', L);
}

// dup_names.txt — top-level names defined in 2+ files (rival owner OR forwarder; check by hand)
{
  const skip = new Set(['main', 'wrapper', 'decorator', 'inner', 'run', 'load', 'save', 'get', 'handle',
    'close', 'build', 'render', 'validate', 'setup', 'clear', 'reset', 'register', 'name', 'key']);
  const m = {};
  fns.filter(f => f.indent === 0).forEach(f => (m[f.name] = m[f.name] || []).push(f));
  const L = [];
  Object.entries(m)
    .filter(([n, v]) => !skip.has(n) && !/^__.*__$/.test(n) && new Set(v.map(f => f.file)).size > 1)
    .sort((a, b) => b[1].length - a[1].length)
    .forEach(([n, v]) => L.push(`${n.padEnd(38)} ${v.map(f => `${f.file}:${f.line}(${f.len})`).join('  ')}`));
  write('dup_names.txt', L);
}

// dead.txt — names with no reference outside their own definition(s).
// References are counted as identifier tokens across py/m/ts/toml/json, so a
// name reached only by string dispatch or a decorator shows up as a false positive.
{
  const files = fs.readFileSync(path.join(OUT, 'files.txt'), 'utf8').split('\n')
    .filter(f => /\.(py|m|tsx?|toml|json)$/.test(f) && !/(^|\/)(dist|out|build|site|docs|examples)\//.test(f) && !f.startsWith('.claude'));
  const all = {}, src = {};
  for (const f of files) {
    let t; try { t = fs.readFileSync(path.join(ROOT, f), 'utf8'); } catch { continue; }
    const test = isTest(f);
    for (const w of t.match(/[A-Za-z_][A-Za-z0-9_]*/g) || []) { all[w] = (all[w] || 0) + 1; if (!test) src[w] = (src[w] || 0) + 1; }
  }
  const defs = {};
  fns.forEach(f => (defs[f.name] = defs[f.name] || []).push(f));
  const rows = [];
  for (const [n, ds] of Object.entries(defs)) {
    if (/^__.*__$/.test(n) || /^(test_|visit_|_h_)/.test(n)) continue;
    const refsAll = (all[n] || 0) - ds.length, refsSrc = (src[n] || 0) - ds.length;
    if (refsSrc > 0) continue;
    rows.push({ n, kind: refsAll > 0 ? 'tests-only' : 'unreferenced', len: ds.reduce((s, f) => s + f.len, 0), where: ds.map(f => `${f.file}:${f.line}`).join(' ') });
  }
  const L = [];
  for (const kind of ['unreferenced', 'tests-only']) {
    const r = rows.filter(x => x.kind === kind).sort((a, b) => b.len - a.len);
    L.push(`== ${kind}: ${r.length} functions, ${r.reduce((s, x) => s + x.len, 0)} lines`);
    r.forEach(x => L.push(`${pad(x.len, 5)} ${x.where} ${x.n}`));
    L.push('');
  }
  write('dead.txt', L);
}

console.log('reports written to', OUT);
