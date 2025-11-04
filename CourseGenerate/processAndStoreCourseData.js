const mysql = require("mysql2/promise");
const dotenv = require("dotenv");
dotenv.config();

const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
};

// select assamese
// select assamese_courses
// select bengali
// select bengali_courses
// select course
// select english
// select english_courses
// select gujarati
// select gujarati_courses
// select hindi
// select hindi_courses
// select kannada
// select kannada_courses
// select marathi
// select marathi_courses
// select odia
// select odia_courses
// select punjabi
// select punjabi_courses
// select tamil
// select tamil_courses
// select telugu
// select telugu_courses
// select urdu
// select urdu_courses
const tableName = "content_imports";
const newTableName = "courses_import_courses";

async function main() {
  const connection = await mysql.createConnection(dbConfig);

  try {
    const [rows] = await connection.execute(`
      SELECT course_title,
             IFNULL(NULLIF(set1, ''), course_title) AS set1,
             set1_desc, set1_thumb,
             set2, set2_desc, set2_thumb,
             set3, set3_desc, set3_thumb,
             set4, set4_desc, set4_thumb,
             set5, set5_desc, set5_thumb,
             set6, set6_desc, set6_thumb,
             set7, set7_desc, set7_thumb,
             set8, set8_desc, set8_thumb,
             set9, set9_desc, set9_thumb,
             set10, set10_desc, set10_thumb,
             do_id,
             domain,
             sub_domain,
             author,
             copyright,
             copyright_year,
             subjects,
             course_keywords,
             course_description,
             course_thumb,
             program,
             content_language,
             target_age_group,
             primary_user
      FROM ${tableName}
      WHERE migrated = 1
      ORDER BY id ASC;
    `);

    if (rows.length === 0) {
      console.log("⚠️ No data found in the migrated table.");
      return;
    }

    const courses = {};

    const parseCSVList = (value) =>
      value ? value.split(",").map((s) => s.trim()).filter(Boolean) : [];

    const cleanString = (value) => (value || "").replace(/\n/g, "").trim();

    const processSetLevel = (row, level, parent, lang) => {
      const setKey = `set${level}`;
      const descKey = `${setKey}_desc`;
      const thumbKey = `${setKey}_thumb`;

      if (row[setKey]) {
        const setName = row[setKey].trim();
        let collection = parent.sets || parent.children;

        let existingSet = collection.find(
          (s) => s.name === setName && s.language === lang
        );

        if (!existingSet) {
          existingSet = {
            name: setName,
            description: cleanString(row[descKey]),
            thumb: row[thumbKey] ? row[thumbKey].trim() : "",
            do_id: null,
            content: [],
            children: [],
            language: lang, // 👈 make sets language-specific
          };
          collection.push(existingSet);
        }

        const nextSetKey = `set${level + 1}`;
        if (row[nextSetKey]) {
          processSetLevel(row, level + 1, existingSet, lang);
        } else if (row.do_id?.trim()) {
          existingSet.content.push(row.do_id.trim());
        }
      } else if (row.do_id?.trim()) {
        if (!parent.content) parent.content = [];
        parent.content.push(row.do_id.trim());
      }
    };

    rows.forEach((row) => {
      const {
        course_title,
        course_thumb,
        domain,
        sub_domain,
        author,
        copyright,
        copyright_year,
        subjects,
        course_keywords,
        course_description,
        program,
        content_language,
        target_age_group,
        primary_user,
      } = row;

      const languages = parseCSVList(content_language);

      languages.forEach((lang) => {
        const courseKey = `${course_title}__${lang}`; // 👈 keep course split per language

        if (!courses[courseKey]) {
          courses[courseKey] = {
            originalTitle: course_title,
            sets: [],
            metadata: {
              domain: domain || "",
              sub_domain: sub_domain || "",
              course_thumb: course_thumb ? course_thumb.trim() : "",
              author: author || "",
              copyright: copyright || "",
              copyright_year: copyright_year || "",
              subjects: parseCSVList(subjects),
              course_keywords: parseCSVList(course_keywords),
              course_description: cleanString(course_description),
              program: program || "",
              content_language: [lang], // 👈 one language only
              target_age_group: parseCSVList(target_age_group),
              primary_user: parseCSVList(primary_user),
            },
          };
        }

        // build sets separately for each language
        processSetLevel(row, 1, courses[courseKey], lang);
      });
    });

    const cleanSets = (sets) =>
      sets
        .map((set) => {
          set.content = (set.content || []).filter(Boolean);
          set.children = cleanSets(set.children || []);
          return set;
        })
        .filter((set) => set.content.length > 0 || set.children.length > 0);

    for (const courseKey in courses) {
      courses[courseKey].sets = cleanSets(courses[courseKey].sets || []);
    }

    // Insert/update into summer_course table
    for (const courseKey in courses) {
      const { originalTitle, sets, metadata } = courses[courseKey];

      const sets_do_id = JSON.stringify({ sets });
      const course_metadata = JSON.stringify(metadata);

      try {
        await connection.execute(
          `INSERT INTO ${newTableName} 
           (course_title, sets_do_id, course_metadata, status)
           VALUES (?, ?, ?, 'pending')
           ON DUPLICATE KEY UPDATE 
              sets_do_id = VALUES(sets_do_id), 
              course_metadata = VALUES(course_metadata), 
              status = 'pending';`,
          [originalTitle, sets_do_id, course_metadata]
        );

        console.log(
          `✅ Data inserted/updated for course: ${originalTitle} [${metadata.content_language}]`
        );
      } catch (error) {
        console.error("❌ Database Error:", error);
      }
    }

    console.log("🎯 All data has been stored successfully.");
  } catch (error) {
    console.error("❌ Script Error:", error);
  } finally {
    connection.end();
  }
}

main();
