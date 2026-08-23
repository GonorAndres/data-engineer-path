window.DashboardPlotly = {
    palette: ['#2563EB', '#059669', '#D97706', '#DC2626', '#7C3AED', '#DB2777', '#0891B2', '#65A30D'],

    baseLayout: {
        font: { family: 'Inter, system-ui, sans-serif', size: 12, color: '#6B7280' },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        margin: { l: 55, r: 16, t: 32, b: 50 },
        xaxis: { gridcolor: '#F3F4F6', zerolinecolor: '#E5E7EB', linecolor: '#E5E7EB', tickfont: { size: 11 } },
        yaxis: { gridcolor: '#F3F4F6', zerolinecolor: '#E5E7EB', linecolor: '#E5E7EB', tickfont: { size: 11 } },
        hoverlabel: { font: { family: 'Inter, system-ui, sans-serif', size: 12 } },
    },

    config: {
        displayModeBar: false,
        responsive: true,
    },
};

function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('-translate-x-full');
    document.getElementById('sidebar-overlay').classList.toggle('hidden');
}

function waitForPlotly(cb) {
    if (window.Plotly) { cb(); return; }
    var t = setInterval(function() { if (window.Plotly) { clearInterval(t); cb(); } }, 50);
}

// Tracked in the DOM's terms, not localStorage's: every page renders EN-first,
// so this starts at 'en' regardless of what a previous page stored.
var currentLang = 'en';

function setLang(lang) {
    // Clicking the already-active language must be a no-op: .lang-enter is
    // added on reveal, and adding it to elements that are already visible
    // would fade the whole page for no reason.
    if (lang === currentLang) return;
    currentLang = lang;
    document.querySelectorAll('[data-lang]').forEach(function(el) {
        var show = el.dataset.lang === lang;
        el.classList.toggle('hidden', !show);
        // The class only needs adding once. `hidden` is display:none, and a
        // CSS animation restarts each time an element returns from
        // display:none -- so later toggles replay the fade on their own.
        if (show) el.classList.add('lang-enter');
    });
    document.getElementById('btn-en').classList.toggle('lang-toggle-active', lang === 'en');
    document.getElementById('btn-es').classList.toggle('lang-toggle-active', lang === 'es');
    localStorage.setItem('dashboard-lang', lang);
    // Chart pages listen for this and re-render their Plotly chrome (axis
    // titles, hover text) and JS-written labels in the new language --
    // data-lang spans cannot reach inside an SVG that Plotly owns. Fires
    // only on a real change, thanks to the guard above.
    document.dispatchEvent(new CustomEvent('langchange', { detail: { lang: lang } }));
}

document.addEventListener('DOMContentLoaded', function() {
    var saved = localStorage.getItem('dashboard-lang');
    if (saved && saved !== 'en') setLang(saved);
});

document.addEventListener('click', function(e) {
    var link = e.target.closest('a[href^="/"]');
    if (!link || link.getAttribute('href') === window.location.pathname) return;
    // One bar at a time, and gone when its animation ends: without the
    // cleanup, every navigation click leaked an invisible fixed div pinned
    // over the top of the viewport at z-index 9999.
    var prev = document.querySelector('.nav-progress');
    if (prev) prev.remove();
    var bar = document.createElement('div');
    bar.className = 'nav-progress';
    bar.addEventListener('animationend', function() { bar.remove(); });
    document.body.appendChild(bar);
});

document.addEventListener('mouseover', function(e) {
    var link = e.target.closest('a[href^="/"]');
    if (!link || link.dataset.prefetched) return;
    link.dataset.prefetched = '1';
    var hint = document.createElement('link');
    hint.rel = 'prefetch';
    hint.href = link.getAttribute('href');
    document.head.appendChild(hint);
});
