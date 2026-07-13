/* GoatCounter analytics — privacy-friendly, no cookies.
   count.js counts the initial pageview itself; Material's instant
   navigation swaps pages without full loads, so subsequent views are
   counted from the location$ stream (skipping its initial emission to
   avoid double-counting the first page). */
(function () {
  var s = document.createElement("script")
  s.async = true
  s.src = "https://gc.zgo.at/count.js"
  s.dataset.goatcounter = "https://narad.goatcounter.com/count"
  document.head.appendChild(s)
})()

if (typeof location$ !== "undefined") {
  var first = true
  location$.subscribe(function (url) {
    if (first) { first = false; return }
    if (window.goatcounter && window.goatcounter.count) {
      goatcounter.count({ path: url.pathname + url.search })
    }
  })
}
