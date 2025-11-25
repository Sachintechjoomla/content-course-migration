import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Brackets } from "typeorm";
import { Content } from "../entities/content.entity";
import { ContentService } from "../content/content.service";
import { ConfigService } from "@nestjs/config";

import fs from "fs";
import path from "path";
import AWS from "aws-sdk";
import axios from "axios";
import https from "https";

@Injectable()
export class CronService {
  private readonly logger = new Logger(CronService.name);

  constructor(
    @InjectRepository(Content)
    private readonly contentRepository: Repository<Content>,
    private readonly contentService: ContentService,
    private readonly configService: ConfigService,

  ) {}

  async downloadFile(limit: number = 1): Promise<void> {
    let documentUrl =
      "https://prathamopenschool.org/CourseContent/FCGames/One_and_many_EN.zip";
    let doId = "do_2142149445604474881181";
    let uniqueCode = "asdasdas";
    let mimeType = "application/vnd.ekstep.html-archive";
    let fileUrl = "";
    let tempFilePath = "";
    const unzipper = require("unzipper");
    const archiver = require("archiver");

    // Step 1: Check if it's a YouTube URL
    if (documentUrl.includes("youtube.com")) {
      console.log(
        "YouTube URL detected, skipping file download and S3 upload."
      );
      fileUrl = documentUrl;
    } else {
      let fileExtension = path.extname(new URL(documentUrl).pathname).slice(1);

      if (
        !["zip", "pdf", "jpg", "png", "docx", "mp4", "m4v"].includes(
          fileExtension
        )
      ) {
        console.warn(`Unsupported file type: ${fileExtension}`);
        return;
      }

      // Step 2: Download Document
      // const agent = new https.Agent({
      //   rejectUnauthorized: false, // ⚠️ Disable SSL certificate validation
      // });
      // Step 2: Download Document
      const agent = new https.Agent({
        rejectUnauthorized: false, // ⚠️ Disable SSL certificate validation
      });

      // Check file size before downloading
      try {
        const headResponse = await axios.head(documentUrl, {
          httpsAgent: agent,
        });

        const contentLength = headResponse.headers["content-length"];
        const fileSizeInMB = parseInt(contentLength || "0") / (1024 * 1024);

        if (fileSizeInMB > 150) {
          console.warn(
            `File too large to download (${fileSizeInMB.toFixed(
              2
            )} MB). Skipping.`
          );
          return;
        }
      } catch (err: any) {
        console.error("Failed to check file size:", err.message);
        return;
      }

      const documentResponse = await axios.get(documentUrl, {
        responseType: "stream",
        httpsAgent: agent,
      });
      tempFilePath = `/tmp/${uniqueCode}.${fileExtension}`;
      const writer = fs.createWriteStream(tempFilePath);
      documentResponse.data.pipe(writer);

      await new Promise((resolve, reject) => {
        writer.on("finish", resolve);
        writer.on("error", reject);
      });

      // Added for m4v support
      // === If file extension is .m4v, rename it to .mp4 ===
      if (fileExtension === "m4v") {
        const newFilePath = `/tmp/${uniqueCode}.mp4`;
        fs.renameSync(tempFilePath, newFilePath);
        tempFilePath = newFilePath;
        fileExtension = "mp4"; // Update to use mp4 for S3 key and logic
        mimeType = "video/mp4"; // Set appropriate MIME type
      }

      // === ZIP FILE LOGIC ===
      if (fileExtension === "zip") {
        console.log(`🚀 Starting optimized ZIP processing for: ${documentUrl}`);

        try {
          // Use optimized streaming ZIP processing
          const JSZip = require("jszip");
          const stream = require("stream");

          // Download ZIP file
          const agent = new (require("https").Agent)({
            rejectUnauthorized: false,
          });
          const response = await axios.get(documentUrl, {
            responseType: "arraybuffer",
            httpsAgent: agent,
            timeout: 300000, // 5 minutes timeout
          });

          console.log(
            `📦 ZIP downloaded, size: ${(
              response.data.length /
              1024 /
              1024
            ).toFixed(2)} MB`
          );

          // Load ZIP with JSZip
          const zip = new JSZip();
          const zipData = await zip.loadAsync(response.data);

          console.log(`📂 Extracting ZIP contents...`);

          // Get all files from ZIP
          const files = Object.keys(zipData.files);
          console.log(`📁 Found ${files.length} files in ZIP`);

          // Check if we need to flatten (single folder containing all files)
          let needsFlattening = false;
          let rootFolder = "";

          if (files.length > 0) {
            const firstFile = files[0];
            const pathParts = firstFile.split("/");
            if (pathParts.length > 1 && pathParts[0] !== "") {
              // Check if all files are in the same root folder
              const potentialRoot = pathParts[0];
              const allInSameFolder = files.every((file) =>
                file.startsWith(potentialRoot + "/")
              );
              if (allInSameFolder) {
                needsFlattening = true;
                rootFolder = potentialRoot;
                console.log(
                  `🔄 Detected nested folder structure, will flatten: ${rootFolder}`
                );
              }
            }
          }

          // Create new ZIP with JSZip
          const newZip = new JSZip();

          // Process each file
          for (const filePath of files) {
            if (zipData.files[filePath].dir) continue; // Skip directories

            const fileData = await zipData.files[filePath].async("uint8array");
            let newPath = filePath;

            // Flatten if needed
            if (needsFlattening && filePath.startsWith(rootFolder + "/")) {
              newPath = filePath.substring(rootFolder.length + 1);
            }

            newZip.file(newPath, fileData);
          }

          console.log(`📦 Re-zipping with optimized settings...`);

          // Generate optimized ZIP stream
          const zipStream = newZip.generateAsync({
            type: "nodebuffer",
            compression: "DEFLATE",
            compressionOptions: {
              level: 1, // Fast compression instead of level 9
            },
          });

          // Create temporary file for S3 upload
          const finalZipPath = `/tmp/${uniqueCode}_cleaned.zip`;
          fs.writeFileSync(finalZipPath, zipStream);

          // Replace tempFilePath with the cleaned ZIP path for S3 upload
          tempFilePath = finalZipPath;

          console.log(`✅ Optimized ZIP processing completed`);
        } catch (error) {
          console.error(`❌ Optimized ZIP processing failed:`, error);
          throw error;
        }
      }

      // === AWS S3 Upload Logic ===
      AWS.config.update({
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        region: process.env.AWS_REGION,
      });

      const s3 = new AWS.S3();
      const bucketName = process.env.AWS_BUCKET_NAME || "";
      const s3Key = `content/assets/${doId}/file.${fileExtension}`;

      // Progress tracking for S3 upload
      let uploadedBytes = 0;
      let lastLoggedMB = 0;
      const progressStream = new (require("stream").Transform)({
        transform(chunk: any, encoding: any, callback: any) {
          uploadedBytes += chunk.length;
          const currentMB = Math.floor(uploadedBytes / (1024 * 1024));
          if (currentMB >= lastLoggedMB + 5) {
            lastLoggedMB = currentMB;
            console.log(`📊 Uploaded: ${currentMB} MB`);
          }
          callback(null, chunk);
        },
      });

      const fileStream = fs.createReadStream(tempFilePath);
      fileStream.pipe(progressStream);

      console.log(`🚀 Starting S3 upload with optimized settings...`);
      console.log(`⚙️  Chunk size: 5MB, Queue size: 4`);

      const uploadResponse = await s3
        .upload(
          {
            Bucket: bucketName,
            Key: s3Key,
            Body: progressStream,
            ContentType: mimeType || "application/octet-stream",
          },
          {
            partSize: 5 * 1024 * 1024, // 5MB chunks
            queueSize: 4, // Increased queue size for better performance
          }
        )
        .promise();

      console.log("Upload successful:", uploadResponse);

      // Step 5: Generate the S3 URL
      fileUrl = `https://${bucketName}.s3-${AWS.config.region}.amazonaws.com/${s3Key}`;

      // Clean up temporary files
      fs.unlinkSync(tempFilePath);
    }

    console.log(fileUrl);
  }

  async processRecords(limit: number = 121): Promise<void> {
    this.logger.log("Cron job started: Fetching records to process...");
    console.log("DEBUG: Cron method triggered");

    // Use QueryBuilder for the NOT EXISTS subquery
    /*
    const records = await this.contentRepository
      .createQueryBuilder('c')
      .where('NOT EXISTS (SELECT 1 FROM Content child WHERE child.parentid = c.content_id)')
      .andWhere('c.repository_id = :repositoryId', { repositoryId: 'SCAPP' })
      .andWhere('c.isdeleted = :isDeleted', { isDeleted: 0 })
      .andWhere('c.migrated = :migrated', { migrated: 0 })
      .take(limit)
      .getMany();
    */

    // We need to update migrated column to 10 for reecords those have set3 column is not null for content repository
    // const recordsToUpdateHasSet3 = await this.contentRepository
    //   .createQueryBuilder("c")
    //   .where("c.set3 IS NOT NULL")
    //   .andWhere("c.migrated = :migrated", { migrated: 0 })
    //   .getMany();
    // if (recordsToUpdateHasSet3.length > 0) {
    //   this.logger.log(
    //     `Updating ${recordsToUpdateHasSet3.length} records with set3 column not null to migrated = 10`
    //   );
    //   await this.contentRepository
    //     .createQueryBuilder()
    //     .update(Content)
    //     .set({ migrated: 10 })
    //     .where("id IN (:...ids)", {
    //       ids: recordsToUpdateHasSet3.map((record: any) => record.id),
    //     })
    //     .execute();
    // } else {
    //   this.logger.log("No records found with set3 column not null.");
    // }

    // Fetch records that are not migrated
    const records = await this.contentRepository
      .createQueryBuilder("c")
      .where("c.migrated IN (:...migrated)", { migrated: [0, 2] })
      // .andWhere("c.id = :id", { id: 1 })
      .take(limit)
      .getMany();

    console.log(records);
    if (records.length === 0) {
      this.logger.log("No records to process.");
      return;
    }

    console.log(records);

    for (const record of records) {
      try {
        // Commented for youth net
        // this.logger.log(`Processing content_id: ${record.content_id}`);
        // Youthnet do not have previous content Ids
        this.logger.log(`Processing content_id: ${record.id}`);

        // console.log(record);

        // Process each record using ContentService
        const result = await this.contentService.processSingleContentRecord(
          record
        );
        // console.log(result);

        if (result) {
          record.migrated = 1; // ✅ Success
        } else {
          record.migrated = 2; // ❌ Failure
        }

        // ✅ Assign only valid string values to `do_id`
        record.do_id = typeof result === "string" ? result : "";

        await this.contentRepository.save(record);

        // Commented for youth net
        // this.logger.log(`Successfully migrated content_id: ${record.content_id}`);

        // Youthnet do not have previous content Ids
        if (record.migrated === 1) {
          this.logger.log(`Successfully migrated content_id: ${record.id}`);
        } else {
          this.logger.error(`Failed to migrate content_id: ${record.id}`);
        }
      } catch (error) {
        this.logger.error(`Error processing content_id ${record.id}:`, error);
      }
    }

    this.logger.log("Cron job completed successfully.");
  }

  // ✅ Standalone function, not a class method
  private async loadValidSubjects(): Promise<string[]> {
    const usedFramework = process.env.USED_FRAMEWORK || "pos";
    // Construct filename dynamically
    const fileName = `${usedFramework}-framework.json`;

    const filePath = path.join(process.cwd(), fileName);
    const jsonData = JSON.parse(fs.readFileSync(filePath, "utf-8"));

    const categories = jsonData.result.framework.categories || [];
    const subjectSet = new Set<string>();

    for (const category of categories) {
      for (const term of category.terms || []) {
        for (const assoc of term.associations || []) {
          if (assoc.category === "subject" && assoc.name) {
            subjectSet.add(assoc.name.trim());
          }
        }
      }
    }

    // Ensure "Sustainable Living" is present
    if (!subjectSet.has("Sustainable Living")) {
      subjectSet.add("Sustainable Living");
    }

    // Ensure "Biodiversity & Conservation" is present
    if (!subjectSet.has("Biodiversity & Conservation")) {
      subjectSet.add("Biodiversity & Conservation");
    }

    // Ensure "Assamese" is present
    if (!subjectSet.has("Assamese")) {
      subjectSet.add("Assamese");
    }

    // Ensure "General health awareness" is present
    if (!subjectSet.has("General health awareness")) {
      subjectSet.add("General health awareness");
    }

    // Ensure "Games" is present
    if (!subjectSet.has("Games")) {
      subjectSet.add("Games");
    }

    return Array.from(subjectSet);
  }

  // ✅ Standalone function, not a class method
  private async loadValidSubDomains(): Promise<string[]> {
    const usedFramework = process.env.USED_FRAMEWORK || "pos";
    // Construct filename dynamically
    const fileName = `${usedFramework}-framework.json`;

    const filePath = path.join(process.cwd(), "pos-framework.json");
    const jsonData = JSON.parse(fs.readFileSync(filePath, "utf-8"));

    const categories = jsonData.result.framework.categories || [];
    const subDomainSet = new Set<string>();

    for (const category of categories) {
      for (const term of category.terms || []) {
        for (const assoc of term.associations || []) {
          if (assoc.category === "subDomain" && assoc.name) {
            subDomainSet.add(assoc.name.trim());
          }
        }
      }
    }

    // Ensure "Academics" is present
    if (!subDomainSet.has("Academics")) {
      subDomainSet.add("Academics");
    }

    // Ensure "Growth & Learning" is present
    if (!subDomainSet.has("Growth & Learning")) {
      subDomainSet.add("Growth & Learning");
    }

    return Array.from(subDomainSet);
  }

  async checkDataValidAndUpdate(): Promise<void> {
    this.logger.log("Cron job started: Fetching records to process...");
    console.log("DEBUG: Cron method checkDataValidAndUpdate");

    const records = await this.contentRepository
      .createQueryBuilder("c")
      .where("c.migrated = :migrated", { migrated: 0 })
      .take(2000)
      .getMany();

    console.log(records);
    console.log("migrated records");
    if (records.length === 0) {
      this.logger.log("No records to process.");
      return;
    }

    const VALID_LANGUAGES = [
      "Assamese",
      "Bengali",
      "English",
      "Gujarati",
      "Hindi",
      "Kannada",
      "Kashmiri",
      "Khasi",
      "Malayalam",
      "Manipuri",
      "Marathi",
      "Odia",
      "Punjabi",
      "Rongdani",
      "Sanskrit",
      "Tamil",
      "Telugu",
      "Urdu",
      "Rabha (Rongdani)",
    ];
    const subjectList = await this.loadValidSubjects();
    const subDomainList = await this.loadValidSubDomains();
    const alreadyAddedData = {
      subjects: subjectList,
      subDomains: subDomainList,
      lanugaes: VALID_LANGUAGES,
    };

    for (const record of records) {
      try {
        this.logger.log(`Processing content_id: ${record.id}`);
        const validate = await this.checkDataValid(record, alreadyAddedData);
        if (!validate) {
          this.logger.warn(
            `Skipping record ${record.id} due to validation failure.`
          );
          continue;
        }
      } catch (error) {
        this.logger.error(`Error processing content_id ${record.id}:`, error);
      }
    }
  }

  private async checkDataValid(single: any, list: any): Promise<boolean> {
    const domainValid = await this.checkDomainValid(single);
    const subjectValid = await this.checkSubjectValid(single, list);
    const subDomainValid = await this.checkSubDomainValid(single, list);
    const languageValid = await this.checkLanguageValid(single, list);
    const programValid = await this.checkProgramValid(single);
    const primaryUserValid = await this.checkPrimaryUserValid(single);

    // Save record regardless of outcome
    await this.contentRepository.save(single);

    return (
      domainValid &&
      subjectValid &&
      subDomainValid &&
      languageValid &&
      programValid &&
      primaryUserValid
    );
  }

  private async checkDomainValid(single: any): Promise<boolean> {
    const validDomains = [
      "Learning for Life",
      "Learning for School",
      "Learning for Work",
    ];

    if (!single.domain || typeof single.domain !== "string") {
      single.comment =
        (single.comment || "") + " Domain is missing or invalid.";
      single.migrated = 4;
      return false;
    }

    const matched = validDomains.find(
      (d) => d.toLowerCase() === single.domain.toLowerCase()
    );

    if (matched) {
      if (matched !== single.domain) {
        single.comment =
          (single.comment || "") +
          ` Domain casing corrected from '${single.domain}' to '${matched}' in table 'content'.`;
        single.domain = matched;
      }
      single.migrated = 3;
      return true;
    } else {
      single.comment =
        (single.comment || "") +
        ` Domain '${single.domain}' not matched in valid domains.`;
      single.migrated = 4;
      return false;
    }
  }

  private async checkSubjectValid(single: any, list: any): Promise<boolean> {
    const validSubjects = list.subjects;

    if (
      !single.subjects ||
      typeof single.subjects !== "string" ||
      !Array.isArray(validSubjects)
    ) {
      single.comment =
        (single.comment || "") + " Subject is missing or invalid.";
      single.migrated = 4;
      return false;
    }

    const matched = validSubjects.find(
      (s) => s.toLowerCase() === single.subjects.toLowerCase()
    );

    if (matched) {
      if (matched !== single.subjects) {
        single.comment =
          (single.comment || "") +
          ` Subject casing corrected from '${single.subjects}' to '${matched}' in table 'content'.`;
        single.subjects = matched;
      }
      single.migrated = 3;
      return true;
    } else {
      single.comment =
        (single.comment || "") +
        ` Subject '${single.subjects}' not matched in valid subjects.`;
      single.migrated = 4;
      return false;
    }
  }

  private async checkSubDomainValid(single: any, list: any): Promise<boolean> {
    const validSubDomains = list.subDomains;

    if (
      !single.sub_domain ||
      typeof single.sub_domain !== "string" ||
      !Array.isArray(validSubDomains)
    ) {
      single.comment =
        (single.comment || "") + " Sub-domain is missing or invalid.";
      single.migrated = 4;
      return false;
    }

    // Split and trim multiple subdomains
    const inputSubDomains = single.sub_domain
      .split(",")
      .map((s: string) => s.trim())
      .filter((s: string) => s.length > 0);

    const correctedSubDomains: string[] = [];
    const invalidSubDomains: string[] = [];

    inputSubDomains.forEach((sub: string) => {
      const match = validSubDomains.find(
        (valid) => valid.toLowerCase() === sub.toLowerCase()
      );

      if (match) {
        correctedSubDomains.push(match);

        if (match !== sub) {
          single.comment =
            (single.comment || "") +
            ` Sub-domain casing corrected from '${sub}' to '${match}' in table 'content'. `;
        }
      } else {
        invalidSubDomains.push(sub);
      }
    });

    if (invalidSubDomains.length > 0) {
      single.comment =
        (single.comment || "") +
        ` Invalid sub-domain(s): ${invalidSubDomains.join(", ")}. `;
      single.migrated = 4;
      return false;
    }

    single.sub_domain = correctedSubDomains.join(", ");
    single.migrated = 3;
    return true;
  }

  private async checkLanguageValid(single: any, list: any): Promise<boolean> {
    const validlangs = list.lanugaes;
    if (
      !single.content_language ||
      typeof single.content_language !== "string"
    ) {
      single.comment =
        (single.comment || "") + " Language is missing or invalid.";
      single.migrated = 4;
      return false;
    }

    const matched = validlangs.find(
      (lang: any) =>
        lang.toLowerCase() === single.content_language.toLowerCase()
    );

    if (matched) {
      if (matched !== single.content_language) {
        single.comment =
          (single.comment || "") +
          ` Language casing corrected from '${single.content_language}' to '${matched}' in table 'content'.`;
        single.content_language = matched;
      }

      single.migrated = 3;
      return true;
    }

    single.comment =
      (single.comment || "") +
      ` Language '${single.content_language}' not matched in valid languages.`;
    single.migrated = 4;
    return false;
  }

  private async checkProgramValid(single: any): Promise<boolean> {
    const VALID_PROGRAMS = [
      "Pragyanpath",
      "Hamara Gaon",
      "Early Childhood Education",
      "Inclusive Education (ENABLE)",
      "Elementary",
      "Second Chance",
      "Digital Initiatives",
      "Vocational Training",
      "Pratham Council For Vulnerable Children",
      "Annual Status of Education Report",
      "Open School",
      "Experimento India",
    ];

    if (!single.program || typeof single.program !== "string") {
      single.comment =
        (single.comment || "") + " Program is missing or invalid.";
      single.migrated = 4;
      return false;
    }

    // Split and trim multiple programs
    const inputPrograms = single.program
      .split(",")
      .map((p: string) => p.trim())
      .filter((p: string) => p.length > 0);

    const correctedPrograms: string[] = [];
    const invalidPrograms: string[] = [];

    inputPrograms.forEach((prog: any) => {
      const match = VALID_PROGRAMS.find(
        (valid) => valid.toLowerCase() === prog.toLowerCase()
      );

      if (match) {
        correctedPrograms.push(match);

        if (match !== prog) {
          single.comment =
            (single.comment || "") +
            ` Program casing corrected from '${prog}' to '${match}' in table 'content'. `;
        }
      } else {
        invalidPrograms.push(prog);
      }
    });

    if (invalidPrograms.length > 0) {
      single.comment =
        (single.comment || "") +
        ` Invalid program(s): ${invalidPrograms.join(", ")}. `;
      single.migrated = 4;
      return false;
    }

    single.program = correctedPrograms.join(", ");
    single.migrated = 3;
    return true;
  }

  private async checkPrimaryUserValid(single: any): Promise<boolean> {
    const VALID_MULTI_VALUES = [
      "Educators",
      "Learners/Children",
      "Parents/Care givers",
    ];

    if (!single.primary_user || typeof single.primary_user !== "string") {
      single.comment =
        (single.comment || "") + " primary_user field is missing or invalid.";
      single.migrated = 4;
      return false;
    }

    // Split and trim input values
    const inputValues = single.primary_user
      .split(",")
      .map((v: any) => v.trim());

    const normalizedSet = new Set<string>();

    for (const val of inputValues) {
      const lowerVal = val.toLowerCase();

      // Normalize to standard categories based on partial matching
      if (lowerVal.includes("educator")) {
        normalizedSet.add("Educators");
      } else if (lowerVal.includes("learn") || lowerVal.includes("child")) {
        normalizedSet.add("Learners/Children");
      } else if (
        lowerVal.includes("parent") ||
        lowerVal.includes("care giver") ||
        lowerVal.includes("caregiver")
      ) {
        normalizedSet.add("Parents/Care givers");
      } else {
        // Unknown/invalid values
        if (!single.comment) single.comment = "";
        single.comment += ` Invalid primary_user value(s): ${val}.`;
        single.migrated = 4;
        return false;
      }
    }

    // Build corrected string from normalized set
    const correctedValues = Array.from(normalizedSet);
    const original = inputValues.join(", ");
    const corrected = correctedValues.join(", ");

    if (original !== corrected) {
      single.comment =
        (single.comment || "") +
        ` primary_user field normalized from '${original}' to '${corrected}' in table 'content'.`;
      single.primary_user = corrected;
    }

    single.migrated = 0;
    return true;
  }

  async updateRecordsChangeCreatedBy(): Promise<any> {
    try {
      this.logger.log("Cron job started: created_by fields updated.");
      await this.contentService.updateRecordsChangeCreatedBy();
    } catch (error) {
      this.logger.error(
        `Error updating created_by `,
        error
      );
    }

    this.logger.log("Cron job completed: created_by fields updated.");
  }

  async deleteAllContents(limit = 100) {
    try {
      // ✅ Array of table names
      const tables = [
        'content_imports'
      ];

      for (const tableName of tables) {
        console.log(`Processing table: ${tableName}`);

        // ✅ Step 1: Fetch rows
      //   const rows = await this.contentRepository.query(`
      //   SELECT id, do_id
      //   FROM ${tableName}
      //   WHERE do_id IS NOT NULL
      //   ORDER BY id
      //   LIMIT ${limit}
      // `);

      //   if (!rows.length) {
      //     console.log(`⚠️ No content found in ${tableName}`);
      //     continue;
      //   }

      const rows = [
        {do_id: 'do_21443567097312051215772', id: '1'},
        {do_id: 'do_21443567159879270415774', id: '2'}
      ];

        console.log(`Found ${rows.length} content in ${tableName}`);

        // ✅ Step 2: Process each row
        for (const row of rows) {
          const doId = row.do_id;

          let success = false;
          let attempts = 0;

          while (!success && attempts < 3) {
            attempts++;
            try {
              const deleteUrl = `${this.configService.get(
                "MIDDLEWARE_URL"
              )}/action/content/v3/retire/${doId}`;

              const deleteRes = await axios.delete(deleteUrl, {
                headers: {
                  Accept: "application/json, text/plain, */*",
                  Authorization: `Bearer ${this.configService.get<string>(
                    "USER_TOKEN"
                  )}`,
                  tenantid: this.configService.get<string>(
                    "MIDDLEWARE_TENANT_ID"
                  ),
                  "X-Channel-Id":
                    this.configService.get<string>("X_CHANNEL_ID"),
                  "Content-Type": "application/json",
                },
              });

              console.log(
                `✅ Deleted content ${doId} (attempt ${attempts})`,
                deleteRes.data
              );
              success = true;

              console.log(`✅ Updated DB row ${row.id} in ${tableName}`);
            } catch (err: any) {
              console.error(
                `❌ Attempt ${attempts} failed to delete course ${doId}:`,
                err.response?.data || err.message
              );

              if (attempts >= 3) {
                console.error(
                  `🚨 Giving up on ${doId} after 3 failed attempts.`
                );
              } else {
                // ✅ Step 3: Update that row
                await this.contentRepository.query(
                  `UPDATE ${tableName}
                    SET do_id = NULL, migrated = 0
                    WHERE id = ?`,
                  [row.id]
                );
                console.log(`⏳ Retrying ${doId}...`);
              }
            }
          }
        }
      }
    } catch (error: any) {
      console.error(`Error in deleteAllcontent: ${error.message}`);
      throw error;
    }
  }

  async updateZipSnapshot(): Promise<any> {
    try {
      this.logger.log("Cron job started: updateZipSnapshot");
      await this.contentService.updateZipSnapshot();
    } catch (error) {
      this.logger.error(`Error updating zip snapshot:`, error);
    }
  }

  async publishAllContentsFromDB(): Promise<any> {
    try {
      this.logger.log("Cron job started: publishAllContentsFromDB");
      await this.contentService.publishAllContentsFromDB([
'english',
'gujrati',
'hindi',
'kannada',
'marathi',
'odia',
'panjabi',
'tamil',
'telugu',
        'bengali'
      ]);
    } catch (error) {
      this.logger.error(`Error publishing all contents from DB:`, error);
    }
  }
}
