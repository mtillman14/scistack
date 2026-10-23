// Publishes the beta banner's rendered height (overrides/main.html announce
// block) as --beta-banner-height, so the sticky header, sidebars and anchor
// targets in docs/stylesheets/extra.css sit below it. The banner wraps onto
// more lines on narrow screens, so its height is measured, not assumed.
(function () {
  var banner = document.querySelector('[data-md-component="announce"]');
  if (!banner) return;
  var root = document.documentElement;
  function publish() {
    root.style.setProperty("--beta-banner-height", banner.offsetHeight + "px");
  }
  publish();
  if ("ResizeObserver" in window) new ResizeObserver(publish).observe(banner);
  else window.addEventListener("resize", publish);
})();
