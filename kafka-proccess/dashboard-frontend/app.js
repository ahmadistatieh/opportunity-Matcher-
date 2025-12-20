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
          ${((s.similarity || 0) * 100).toFixed(1)}%
        </div>
      </div>
    `;

    resultsList.appendChild(card);
  });

  resultsCountEl.textContent = students.length;
}

async function readErrorSafe(res) {
  try {
    const text = await res.text();
    return text || `HTTP ${res.status}`;
  } catch {
    return `HTTP ${res.status}`;
  }
}

uploadForm.addEventListener("submit", async (e) => {
  e.preventDefault();

  const file = opportunityFileInput.files?.[0];
  if (!file) {
    setStatus("❌ الرجاء اختيار ملف", "error");
    return;
  }

  try {
    setStatus("⏳ جاري تشغيل المطابقة...", "success");

    const isJson = file.name.toLowerCase().endsWith(".json");

    let res;

    if (isJson) {
      // Existing behavior: JSON -> /match_students
      const text = await file.text();
      const payload = JSON.parse(text);

      res = await fetch(`${API_BASE_URL}/match_students`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

    } else {
      // New behavior: PDF/DOC/DOCX/TXT -> /match_students_file (multipart/form-data)
      const formData = new FormData();
      formData.append("file", file);

      res = await fetch(`${API_BASE_URL}/match_students_file`, {
        method: "POST",
        body: formData
      });
    }

    if (!res.ok) {
      const errTxt = await readErrorSafe(res);
      setStatus(`❌ فشل الطلب: ${errTxt}`, "error");
      return;
    }

    const students = await res.json();
    renderStudents(students);
    setStatus("✅ تم جلب النتائج بنجاح", "success");

  } catch (err) {
    setStatus("❌ خطأ في قراءة الملف أو الاتصال بالخادم", "error");
    console.error(err);
  }
});
