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

function setLang(lang) {
    document.querySelectorAll('[data-lang]').forEach(function(el) {
        el.classList.toggle('hidden', el.dataset.lang !== lang);
    });
    document.getElementById('btn-en').classList.toggle('lang-toggle-active', lang === 'en');
    document.getElementById('btn-es').classList.toggle('lang-toggle-active', lang === 'es');
    localStorage.setItem('dashboard-lang', lang);
}

document.addEventListener('DOMContentLoaded', function() {
    var saved = localStorage.getItem('dashboard-lang');
    if (saved && saved !== 'en') setLang(saved);
});

document.addEventListener('click', function(e) {
    var link = e.target.closest('a[href^="/"]');
    if (!link || link.getAttribute('href') === window.location.pathname) return;
    var bar = document.createElement('div');
    bar.className = 'nav-progress';
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
