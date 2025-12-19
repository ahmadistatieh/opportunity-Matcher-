const API_BASE_URL = "http://localhost:8080";

const uploadForm = document.getElementById("uploadForm");
const opportunityFileInput = document.getElementById("opportunityFile");
const statusEl = document.getElementById("status");
const resultsList = document.getElementById("resultsList");
const resultsCountEl = document.getElementById("resultsCount");

function setStatus(msg, type = "") {
  statusEl.textContent = msg;
  statusEl.className = `status ${type}`;
}

opportunityFileInput.addEventListener("change", () => {
  if (opportunityFileInput.files.length > 0) {
    setStatus("✅ تم تحميل الملف بنجاح", "success");
  }
});

function buildPayloadFromText(text) {
  const lower = text.toLowerCase();

  const skills = [];
  if (lower.includes("java")) skills.push("Java");
  if (lower.includes("spark")) skills.push("Spark");
  if (lower.includes("sql")) skills.push("SQL");
  if (lower.includes("python")) skills.push("Python");

  const courses = [];
  if (lower.includes("algorithms")) courses.push("Algorithms");
  if (lower.includes("database")) courses.push("Database");

  return {
    skills,
    courses,
    major: "",
    minGpa: null,
    minSimilarity: null
  };
}

function renderStudents(students) {
  resultsList.innerHTML = "";

  if (!students || students.length === 0) {
    resultsList.innerHTML = `
      <div class="empty-state">
        <h3>لا توجد نتائج مطابقة</h3>
      </div>
    `;
    resultsCountEl.textContent = "0";
    return;
  }

  students.forEach((s, i) => {
    const card = document.createElement("div");
    card.className = "student-card";

    card.innerHTML = `
      <div>
        <div class="student-name">${i + 1}. ${s.full_name}</div>
        <div class="student-email">${s.email}</div>
        <div class="student-meta">
          <div>التخصص: ${s.major || "-"}</div>
          <div>GPA: ${s.gpa_d != null ? s.gpa_d : "-"}</div>
          <div>التدريب: ${s.training != null ? s.training : "-"}</div>
          <div>ساعات الدراسة: ${s.studying_hours != null ? s.studying_hours : "-"}</div>
        </div>
      </div>

      <div class="chips">
        ${(s.skills_arr || []).map(sk => `<span class="chip skill">${sk}</span>`).join("")}
        ${(s.courses_arr || []).map(c => `<span class="chip course">${c}</span>`).join("")}
      </div>

      <div class="similarity-badge">
        <div class="similarity-pill">
          ${(s.similarity * 100).toFixed(1)}%
        </div>
      </div>
    `;

    resultsList.appendChild(card);
  });

  resultsCountEl.textContent = students.length;
}

uploadForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  try {
    const file = opportunityFileInput.files[0];
    const text = await file.text();

    let payload;

    if (file.name.endsWith(".json")) {
      payload = JSON.parse(text);
    } else {
      payload = buildPayloadFromText(text);
    }

    setStatus("⏳ جاري تشغيل المطابقة...", "success");

    const res = await fetch(`${API_BASE_URL}/match_students`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const students = await res.json();
    renderStudents(students);

    setStatus("تم جلب النتائج بنجاح", "success");
  } catch (err) {
    setStatus("خطأ في قراءة الملف أو الاتصال بالخادم", "error");
  }
});
