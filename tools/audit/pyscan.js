// Rough Python source scanner (indent-based, no real parser) — see README.md.
// Emits JSON: functions (span, params), imports (incl. multi-line, lazy), markers.
const fs = require('fs');
const path = require('path');
const files = fs.readFileSync(process.argv[2], 'utf8').split('\n').filter(f => f.endsWith('.py'));
const ROOT = process.env.AUDIT_ROOT || process.cwd();
const PKGS = new Set(['scifor','scimatlab','sciduckdb','scidb','scilineage','scihist','scistack','scistacklog','scistackplot','scistackplotdb','scicanonicalhash']);
const ownerOf = f => ({'scidb-net':'scidbnet','scistack-gui':'scistack_gui'}[f.split('/')[0]] || f.split('/')[0]);

const out = { functions: [], imports: [], markers: [], lazyImports: {}, broadExcept: [] };
const indentOf = s => s.match(/^\s*/)[0].replace(/\t/g, '    ').length;

for (const f of files) {
  const lines = fs.readFileSync(path.join(ROOT, f), 'utf8').split('\n');
  const own = ownerOf(f);
  let lazy = 0;
  // functions
  const stack = [];
  const close = (i, ind) => {
    while (stack.length && ind <= stack[stack.length - 1].indent) {
      const fn = stack.pop();
      let end = i - 1; while (end > fn.line && !lines[end - 1].trim()) end--;
      fn.len = end - fn.line + 1; out.functions.push(fn);
    }
  };
  for (let i = 0; i < lines.length; i++) {
    const L = lines[i]; const t = L.trim();
    if (!t || t.startsWith('#')) continue;
    const ind = indentOf(L);
    // continuation lines (inside parens of a signature etc) are rough; only close on code at lower indent
    const m = t.match(/^(async\s+)?def\s+([A-Za-z_0-9]+)\s*\(/);
    if (m || /^class\s/.test(t)) close(i + 1, ind); else if (stack.length && ind <= stack[stack.length-1].indent && !/^[)\]}]/.test(t)) close(i + 1, ind);
    if (m) {
      // gather signature
      let sig = '', j = i, depth = 0, started = false;
      for (; j < lines.length; j++) {
        for (const ch of lines[j]) { if (ch === '(') { depth++; started = true; } else if (ch === ')') depth--; }
        sig += lines[j] + '\n'; if (started && depth === 0) break;
      }
      const inner = sig.slice(sig.indexOf('(') + 1, sig.lastIndexOf(')'));
      // count top-level commas
      let d = 0, n = 0, any = false;
      for (const ch of inner.replace(/#.*$/gm, '')) { if ('([{'.includes(ch)) d++; else if (')]}'.includes(ch)) d--; else if (ch === ',' && d === 0) n++; if (!/\s|,/.test(ch)) any = true; }
      const parts = inner.replace(/#.*$/gm,'').split(',').map(s=>s.trim()).filter(Boolean);
      const params = parts.filter(p => !/^(self|cls|\*|\/)$/.test(p.split(/[:=]/)[0].trim())).length;
      stack.push({ file: f, name: m[2], line: i + 1, indent: ind, params, nested: stack.length });
    }
    // imports
    const im = t.match(/^from\s+([A-Za-z_][\w.]*)\s+import\s+(.*)$/) || t.match(/^import\s+([A-Za-z_][\w.]*)/);
    if (im) {
      if (ind > 0 && !/^(try|except)/.test(t)) lazy++;
      let names = im[2] || '';
      if (names.startsWith('(')) { let k = i; while (!names.includes(')') && k + 1 < lines.length) names += ' ' + lines[++k].trim(); }
      names = names.replace(/[()]/g, '').replace(/#.*/g, '');
      const pkg = im[1].split('.')[0];
      if (PKGS.has(pkg) && pkg !== own) {
        const priv = names.split(',').map(s => s.trim().split(/\s+as\s+/)[0]).filter(s => /^_[a-z]/.test(s));
        const privMod = im[1].split('.').some(p => p.startsWith('_') && !p.startsWith('__'));
        out.imports.push({ file: f, line: i + 1, own, pkg, mod: im[1], priv, privMod, lazy: ind > 0 });
      }
    }
    // markers
    const c = L.match(/#(.*)$/);
    if (c && /\b(trap|silently|hack|workaround|TODO|FIXME|XXX|kludge)\b/i.test(c[1])) out.markers.push({ file: f, line: i + 1, text: c[1].trim().slice(0, 140) });
    if (/^except\s*(Exception)?\s*(as\s+\w+)?\s*:/.test(t)) {
      // is body just pass / return None / continue?
      let k = i + 1; while (k < lines.length && !lines[k].trim()) k++;
      const body = (lines[k] || '').trim();
      out.broadExcept.push({ file: f, line: i + 1, swallow: /^(pass|continue|return( None)?|return \[\]|return \{\}|return False)$/.test(body) });
    }
  }
  close(lines.length + 1, -1);
  out.lazyImports[f] = lazy;
}
fs.writeFileSync(process.argv[3], JSON.stringify(out));
console.log('functions', out.functions.length, 'xpkg imports', out.imports.length, 'markers', out.markers.length, 'broad excepts', out.broadExcept.length);
