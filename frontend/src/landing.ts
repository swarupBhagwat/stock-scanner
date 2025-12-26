// ===============================
// LANDING PAGE LOGIC
// ===============================

// -------- OPEN SCANNER --------
document.getElementById("openApp")?.addEventListener("click", () => {
  window.location.href = "/app.html";
});

// -------- SMOOTH SCROLL --------
document.getElementById("learnMore")?.addEventListener("click", () => {
  document
    .getElementById("how")
    ?.scrollIntoView({ behavior: "smooth" });
});

// -------- MARKET STATUS --------
function updateMarketStatus() {
  const el = document.getElementById("marketStatus");
  if (!el) return;

  const now = new Date();

  // Convert to IST
  const istTime = new Date(
    now.toLocaleString("en-US", { timeZone: "Asia/Kolkata" })
  );

  const day = istTime.getDay(); // 0 = Sunday, 6 = Saturday
  const hours = istTime.getHours();
  const minutes = istTime.getMinutes();

  const isWeekend = day === 0 || day === 6;

  // NSE timing: 9:15 AM to 3:30 PM
  const isMarketOpen =
    !isWeekend &&
    (hours > 9 || (hours === 9 && minutes >= 15)) &&
    (hours < 15 || (hours === 15 && minutes <= 30));

  if (isMarketOpen) {
    el.textContent = "🟢 Market Open (Live session)";
    el.classList.add("open");
    el.classList.remove("closed");
  } else {
    el.textContent = "🟠 Market Closed (EOD data)";
    el.classList.add("closed");
    el.classList.remove("open");
  }
}

// Run once on load
updateMarketStatus();
