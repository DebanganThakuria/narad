/* Signal Line: the navigation drawer from the keyboard.

   Material opens its drawer with a <label for="__drawer">, which the
   keyboard cannot reach. overrides/partials/header.html puts a real
   button there instead; this script makes it toggle the same checkbox
   Material watches, keeps aria-expanded in step, moves focus into the
   drawer when it opens and closes it again on Escape. Everything is
   delegated from document, so it survives instant navigation. */
(function () {
  function drawer() {
    return document.getElementById("__drawer");
  }

  function sync() {
    var toggle = drawer();
    if (!toggle) return;
    var buttons = document.querySelectorAll(".nr-drawer-button");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].setAttribute("aria-expanded", toggle.checked ? "true" : "false");
    }
  }

  document.addEventListener("click", function (event) {
    var button = event.target.closest && event.target.closest(".nr-drawer-button");
    var toggle = drawer();
    if (!button || !toggle) return;
    /* A click on the checkbox flips it and fires change, as the label did */
    toggle.click();
    if (toggle.checked) {
      var first = firstVisibleLink();
      if (first) first.focus();
    }
  });

  /* The drawer opens on the current section, drawn over the top level, so
     the first link in the DOM can be covered: take the first one that is
     actually on top where it sits */
  function firstVisibleLink() {
    var links = document.querySelectorAll(".md-sidebar--primary a[href]");
    for (var i = 0; i < links.length; i++) {
      var box = links[i].getBoundingClientRect();
      if (!box.width || !box.height || box.left < 0 || box.top < 0 || box.bottom > window.innerHeight) continue;
      var hit = document.elementFromPoint(box.left + box.width / 2, box.top + box.height / 2);
      if (hit && (hit === links[i] || links[i].contains(hit))) return links[i];
    }
    return null;
  }

  document.addEventListener("change", function (event) {
    if (event.target && event.target.id === "__drawer") sync();
  });

  document.addEventListener("keydown", function (event) {
    var toggle = drawer();
    if (event.key !== "Escape" || !toggle || !toggle.checked) return;
    toggle.click();
    var button = document.querySelector(".nr-drawer-button");
    if (button) button.focus();
  });

  sync();
})();
