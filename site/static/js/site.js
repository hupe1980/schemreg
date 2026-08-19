/* schemreg site behaviour: theme toggle, copy buttons, ⌘K search, TOC spy.
 *
 * No framework and no build step. Every feature degrades to a working page if
 * this file fails to load — the theme still follows the OS, code is still
 * selectable, and the search button is simply inert.
 */
(function () {
  'use strict';

  // ── Theme ───────────────────────────────────────────────────────────────
  var root = document.documentElement;

  function currentTheme() {
    if (root.dataset.theme) return root.dataset.theme;
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  var toggle = document.querySelector('[data-theme-toggle]');
  if (toggle) {
    toggle.addEventListener('click', function () {
      var next = currentTheme() === 'dark' ? 'light' : 'dark';
      root.dataset.theme = next;
      try { localStorage.setItem('theme', next); } catch (e) {}
      syncSyntaxSheets(next);
    });
  }

  // The two syntax stylesheets are selected by `prefers-color-scheme`, which an
  // explicit toggle has to override by hand.
  function syncSyntaxSheets(theme) {
    var light = document.getElementById('syntax-light');
    var dark = document.getElementById('syntax-dark');
    if (!light || !dark) return;
    light.media = theme === 'light' ? 'all' : 'not all';
    dark.media = theme === 'dark' ? 'all' : 'not all';
  }
  if (root.dataset.theme) syncSyntaxSheets(root.dataset.theme);

  // ── Copy buttons ────────────────────────────────────────────────────────
  document.querySelectorAll('[data-copy]').forEach(function (btn) {
    btn.addEventListener('click', function () {
      var target = document.querySelector(btn.dataset.copy);
      if (!target || !navigator.clipboard) return;
      navigator.clipboard.writeText(target.textContent.trim()).then(function () {
        var original = btn.textContent;
        btn.textContent = 'Copied';
        btn.setAttribute('data-copied', '');
        setTimeout(function () {
          btn.textContent = original;
          btn.removeAttribute('data-copied');
        }, 1600);
      });
    });
  });

  // Every fenced code block in the prose gets one too.
  document.querySelectorAll('.prose pre').forEach(function (pre) {
    if (!navigator.clipboard) return;
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'copy-btn code-copy';
    btn.textContent = 'Copy';
    btn.setAttribute('aria-label', 'Copy code to clipboard');
    pre.style.position = 'relative';
    btn.style.cssText = 'position:absolute;top:.5rem;inset-inline-end:.5rem;opacity:0;transition:opacity .12s';
    pre.appendChild(btn);
    pre.addEventListener('mouseenter', function () { btn.style.opacity = '1'; });
    pre.addEventListener('mouseleave', function () { if (!btn.hasAttribute('data-copied')) btn.style.opacity = '0'; });
    btn.addEventListener('focus', function () { btn.style.opacity = '1'; });
    btn.addEventListener('click', function () {
      var code = pre.querySelector('code');
      navigator.clipboard.writeText((code || pre).textContent.trim()).then(function () {
        btn.textContent = 'Copied';
        btn.setAttribute('data-copied', '');
        setTimeout(function () {
          btn.textContent = 'Copy';
          btn.removeAttribute('data-copied');
          btn.style.opacity = '0';
        }, 1600);
      });
    });
  });

  // ── Table of contents scroll spy ────────────────────────────────────────
  var tocLinks = Array.prototype.slice.call(document.querySelectorAll('.toc a[href^="#"]'));
  if (tocLinks.length && 'IntersectionObserver' in window) {
    var byId = {};
    tocLinks.forEach(function (a) { byId[decodeURIComponent(a.hash.slice(1))] = a; });

    var headings = Object.keys(byId)
      .map(function (id) { return document.getElementById(id); })
      .filter(Boolean);

    var visible = new Set();
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) visible.add(entry.target.id);
        else visible.delete(entry.target.id);
      });
      var first = headings.find(function (h) { return visible.has(h.id); });
      tocLinks.forEach(function (a) { a.classList.remove('active'); });
      if (first && byId[first.id]) byId[first.id].classList.add('active');
    }, { rootMargin: '-80px 0px -70% 0px', threshold: 0 });

    headings.forEach(function (h) { observer.observe(h); });
  }

  // ── Search ──────────────────────────────────────────────────────────────
  var modal = document.querySelector('[data-search-modal]');
  var input = document.querySelector('[data-search-input]');
  var results = document.querySelector('[data-search-results]');
  var index = null;
  var active = -1;

  function buildIndex() {
    if (index || typeof window.elasticlunr === 'undefined' || !window.searchIndex) return;
    index = window.elasticlunr.Index.load(window.searchIndex);
  }

  function openSearch() {
    if (!modal) return;
    buildIndex();
    modal.hidden = false;
    input.value = '';
    render([]);
    input.focus();
  }

  function closeSearch() {
    if (!modal) return;
    modal.hidden = true;
    active = -1;
  }

  function render(items) {
    if (!results) return;
    results.innerHTML = '';
    active = -1;
    if (!items.length) {
      if (input.value.trim()) {
        results.innerHTML = '<p class="search-empty">No matches.</p>';
      }
      return;
    }
    items.forEach(function (item) {
      var a = document.createElement('a');
      a.href = item.url;
      a.innerHTML = '<strong></strong><span></span>';
      a.querySelector('strong').textContent = item.title;
      a.querySelector('span').textContent = item.body;
      results.appendChild(a);
    });
  }

  function search(term) {
    buildIndex();
    if (!index || !term.trim()) return render([]);
    var hits = index.search(term, {
      bool: 'AND',
      fields: { title: { boost: 3 }, body: { boost: 1 } },
      expand: true
    }).slice(0, 8);

    render(hits.map(function (hit) {
      var doc = hit.doc || {};
      return {
        url: hit.ref,
        title: doc.title || hit.ref,
        body: (doc.body || '').replace(/\s+/g, ' ').slice(0, 110)
      };
    }));
  }

  function move(delta) {
    var links = results.querySelectorAll('a');
    if (!links.length) return;
    if (active >= 0) links[active].classList.remove('active');
    active = (active + delta + links.length) % links.length;
    links[active].classList.add('active');
    links[active].scrollIntoView({ block: 'nearest' });
  }

  document.querySelectorAll('[data-search-open]').forEach(function (b) {
    b.addEventListener('click', openSearch);
  });
  document.querySelectorAll('[data-search-close]').forEach(function (b) {
    b.addEventListener('click', closeSearch);
  });

  if (input) {
    var debounce;
    input.addEventListener('input', function () {
      clearTimeout(debounce);
      debounce = setTimeout(function () { search(input.value); }, 90);
    });
    input.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown') { e.preventDefault(); move(1); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); move(-1); }
      else if (e.key === 'Enter') {
        var links = results.querySelectorAll('a');
        var target = active >= 0 ? links[active] : links[0];
        if (target) { e.preventDefault(); window.location.href = target.href; }
      }
    });
  }

  document.addEventListener('keydown', function (e) {
    if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      modal && modal.hidden ? openSearch() : closeSearch();
    } else if (e.key === 'Escape' && modal && !modal.hidden) {
      closeSearch();
    } else if (e.key === '/' && modal && modal.hidden &&
               !/^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName)) {
      e.preventDefault();
      openSearch();
    }
  });
})();
