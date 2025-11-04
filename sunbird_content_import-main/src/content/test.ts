import { Injectable, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { parse } from "csv-parse/sync";
import axios from "axios";
import * as fs from "fs";
import * as AWS from "aws-sdk";
import FormData from "form-data";
import * as path from "path";
import { CourseService } from "../course/course.service";
import { Content } from "../entities/content.entity";
import { AxiosError } from "axios";
import { DataSource } from "typeorm";
import { InjectDataSource } from "@nestjs/typeorm";
import { FileLoggerService } from "../logger/file-logger.service";
import https from "https";
import type { AxiosResponse } from "axios";
import fileType from "file-type";

@Injectable()
export class ContentService {
  private readonly middlewareUrl: string;
  private readonly frontendURL: string;
  private readonly framework: string;
  private readonly logger = new Logger(ContentService.name); // Define the logger
  private logFilePath: string;

  constructor(
    private readonly configService: ConfigService,
    private readonly courseService: CourseService, // Inject CourseService here
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly fileLogger: FileLoggerService
  ) {
    this.middlewareUrl = this.configService.get<string>("MIDDLEWARE_URL") || "";
    this.frontendURL = this.configService.get<string>("FRONTEND_URL") || "";
    this.framework =
      this.configService.get<string>("FRAMEWORK") || "scp-framework";
    this.logFilePath = path.join(process.cwd(), "error.log"); // Places error.log outside dist/content, beside src
  }

  /**
   * Method to process a single content record
   * @param record - Content entity object
   */
  async processSingleContentRecord(
    record: Content
  ): Promise<string | undefined | false> {
    // Log the start of the process
    this.logger.log(`Processing content record with ID: ${Content}`);

    const title = record.cont_title;
    const fileDownloadURL = record.cont_dwurl || "";

    const isMediaFile = fileDownloadURL.match(/\.(m4a|m4v)$/i); // Checks if fileUrl ends with m4a or m4v

    const fileUrl = isMediaFile
      ? record.convertedUrl || fileDownloadURL
      : fileDownloadURL;

    const primaryCategory = "Learning Resource";
    const userId = this.configService.get<string>("USER_ID") || "";
    const userToken = this.configService.get<string>("USER_TOKEN") || "";
    let appIcon = "";

    if (!title || !fileUrl) {
      return false;
    }

    const isValidFile = await this.validateFileUrl(fileUrl, record);

    if (!title || !fileUrl || !isValidFile) {
      return false;
    }

    const createdContent = await this.createAndUploadContent(
      record,
      title,
      userId,
      fileUrl,
      primaryCategory,
      userToken
    );

    if (!createdContent) {
      return;
    }

    console.log("content created successfully.");

    if (createdContent) {
      //  Step 2: Upload Media
      const uploadedContent = await this.uploadContent(
        createdContent.doId,
        createdContent.fileUrl,
        userToken
      );
      // console.log("Uploaded Content:", uploadedContent);
      if (!uploadedContent) {
        console.log(
          `⚠️ Upload failed for content ID: ${createdContent.doId}. Skipping further steps.`
        );
        return;
      }

      // Step 3: Review Content
      const reviewedContent = await this.reviewContent(
        createdContent.doId,
        userToken
      );
      // console.log("Reviewed Content:", reviewedContent);

      // Step 4: Publish Content
      const publishedContent = await this.publishContent(
        createdContent.doId,
        userToken
      );
      // console.log("published Content:", publishedContent);

      if (publishedContent) {
        // Return Do Id when published content is returned.
        return createdContent.doId;
      }
    }
  }

  /*
  private async getOrCreateCourse(courseName: string): Promise<Course> {
    // Check if course exists; create if not
   // const existingCourse = await this.ContentService.getCourseByName(courseName);
    // return existingCourse || 
    //this.ContentService.createCourse(courseName);
  }
  */

  private getHeaders(userToken: string): Record<string, string> {
    return {
      Authorization: `Bearer ${userToken}`, // Ensure no undefined Authorization
      tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID") || "", // Fallback value
      "X-Channel-Id":
        this.configService.get<string>("X_CHANNEL_ID") || "qa-scp-channel",
      "Content-Type": "application/json",
    };
  }

  private getUrl(endpoint: string): string {
    return `${this.middlewareUrl}${endpoint}`;
  }

  private async createAndUploadContent(
    record: Content,
    title: string,
    userId: string,
    documentUrl: string,
    primaryCategory: string,
    userToken: string
  ) {
    let tempFilePath: string | null = null;
    try {
      const { v4: uuidv4 } = require("uuid");
      const path = require("path");
      const mime = require("mime-types"); // Ensure mime-types is installed
      const SUPPORTED_FILE_TYPES = ["pdf", "mp4", "zip", "mp3", "m4v"];

      const YOUTUBE_URL_REGEX =
        /^(https?:\/\/)?(www\.)?(youtube\.com|youtu\.be)\/.+$/;

      const isYouTubeURL = YOUTUBE_URL_REGEX.test(documentUrl);

      const uniqueCode = uuidv4();
      let fileUrl: string = documentUrl; // Default to documentUrl
      let iconUrl: string = ''; // Default to documentUrl

      const contentLanguage = record.content_language || "";
      const description = record.resource_desc || "";
      const DOMAIN: string | undefined = record.domain;
      const PRIMARY_USER: string | undefined = record.primary_user;
      const PROGRAM: string | undefined = record.program;
      const SUB_DOMAIN: string | undefined = record.sub_domain;
      const SUBJECTS: string | undefined = record.subjects;
      // const KEYWORDS: string | undefined = record.course_keywords;
      const TARGET_AGE_GROUP: string | undefined = record.target_age_group;
      const old_system_content_id = record.old_system_content_id || "";
      // Framework fields

      // Function to handle comma-separated values and convert them into arrays
      const toArray = (value: string | undefined): string[] =>
        value ? value.split(",").map((item) => item.trim()) : [];

      const additionalFields = {
        description: description,
        domain: toArray(DOMAIN),
        primaryUser: toArray(PRIMARY_USER),
        program: toArray(PROGRAM),
        subDomain: toArray(SUB_DOMAIN),
        targetAgeGroup: toArray(TARGET_AGE_GROUP),
        subject: toArray(SUBJECTS),
        // keywords: toArray(KEYWORDS),
        contentLanguage: contentLanguage,
        isContentMigrated: 1,
        oldSystemContentId: old_system_content_id,
        contentType: "Resource",
      };

      const originalUrl = record.cont_dwurl || documentUrl;
      const extFromOriginal = path
        .extname(new URL(originalUrl).pathname)
        .slice(1)
        .toLowerCase();
      const extFromDownloadLink = path
        .extname(new URL(documentUrl).pathname)
        .slice(1)
        .toLowerCase();
      let fileExtension = extFromOriginal || extFromDownloadLink;

      if (fileExtension === "m4v") {
        fileExtension = "mp4";
      }

      // ✅ Check if it's a Google Drive URL
      const googleDriveMatch = /drive\.google\.com\/file\/d\/([^/]+)\//.exec(
        documentUrl
      );
      if (googleDriveMatch) {
        const fileId = googleDriveMatch[1];
        const apiKey = process.env.GOOGLE_DRIVE_API_KEY;

        // First try using the Google Drive API to fetch metadata
        try {
          const metadata = await axios.get(
            `https://www.googleapis.com/drive/v3/files/${fileId}`,
            {
              params: {
                fields: "name,mimeType",
                key: apiKey,
              },
            }
          );

          const { name, mimeType } = metadata.data;
          const extFromName = path.extname(name).slice(1).toLowerCase();
          const extFromMime = mime.extension(mimeType);
          fileExtension = extFromName || extFromMime || "";

          // ✅ Update documentUrl to stream raw file
          documentUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${apiKey}`;
          console.log(`📁 Updated documentUrl for streaming: ${documentUrl}`);
        } catch (err: any) {
          console.warn(
            `⚠️ Google Drive API failed (likely not shared publicly): ${err.response?.status} - ${err.message}`
          );

          // 🔁 Fallback to `uc?export=download`
          documentUrl = `https://drive.google.com/uc?export=download&id=${fileId}`;
          try {
            const response = await axios.get(documentUrl, {
              responseType: "stream",
              timeout: 15000,
            });
            const fileTypeResult = await fileType.fromStream(response.data);
            if (fileTypeResult) {
              fileExtension = fileTypeResult.ext.toLowerCase();
              console.log(
                `✅ Inferred file type from stream: ${fileExtension}`
              );
            } else {
              console.warn(
                `⚠️ Could not infer file type from stream. Defaulting to 'pdf'.`
              );
              fileExtension = "pdf";
            }
          } catch (streamErr) {
            if (streamErr instanceof Error) {
              console.warn(
                `❌ Fallback failed for Google Drive fileId: ${fileId} - ${streamErr.message}`
              );
            } else {
              console.warn(
                `❌ Fallback failed for Google Drive fileId: ${fileId}`,
                streamErr
              );
            }
          }
        }
      }

      /*
        const googleDriveMatch = /drive\.google\.com\/file\/d\/([^/]+)\//.exec(documentUrl);
        if (googleDriveMatch) {
          try {
            const fileId = googleDriveMatch[1];
            const apiKey = process.env.GOOGLE_DRIVE_API_KEY;

            // Get metadata for extension
            const metadata = await axios.get(
              `https://www.googleapis.com/drive/v3/files/${fileId}`,
              {
                params: {
                  fields: 'name,mimeType',
                  key: apiKey,
                },
              }
            );

            const { name, mimeType } = metadata.data;
            console.log(`📁 Google Drive API: name = ${name}, mimeType = ${mimeType}`);

            const extFromName = path.extname(name).slice(1).toLowerCase();
            const extFromMime = mime.extension(mimeType);

            fileExtension = extFromName || extFromMime || '';

            // ✅ Update documentUrl to stream raw file
            documentUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${apiKey}`;
            console.log(`📁 Updated documentUrl for streaming: ${documentUrl}`);
          } catch (err: unknown) {
            if (err instanceof Error) {
              console.warn(`⚠️ Google Drive API lookup failed: ${err.message}`);
            } else {
              console.warn(`⚠️ Google Drive API lookup failed:`, err);
            }
          }
        }
        */
      /* commented 5th may
        const googleDriveMatch = /drive\.google\.com\/file\/d\/([^/]+)\//.exec(documentUrl);
        if (googleDriveMatch) {
          const fileId = googleDriveMatch[1];
          const apiKey = process.env.GOOGLE_DRIVE_API_KEY;
        
          // Attempt to fetch MIME type using Google Drive API
          try {
            const metadata = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}`, {
              params: {
                fields: 'name,mimeType',
                key: apiKey,
              },
            });
        
            const { name, mimeType } = metadata.data;
            console.log(`📁 Google Drive API: name = ${name}, mimeType = ${mimeType}`);
        
            const extFromName = path.extname(name).slice(1).toLowerCase();
            const extFromMime = mime.extension(mimeType);
        
            fileExtension = extFromName || extFromMime || '';
            documentUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${apiKey}`;
            console.log(`📁 Updated documentUrl for streaming: ${documentUrl}`);
          } catch (err: any) {
            console.warn(`⚠️ Google Drive API lookup failed: ${err.response?.status} - ${err.message}`);
            fileExtension = 'pdf'; // fallback
          }
        }
      */

      // 🔄 Fallback to HEAD request if still unknown
      // ✅ Smarter fallback using HEAD request only if extension is missing or useless
      const knownBadExtensions = ["bin", "", undefined];

      if (!fileExtension || knownBadExtensions.includes(fileExtension)) {
        try {
          const headResponse = await axios.head(documentUrl, { timeout: 5000 });
          const mimeTypeFromHead = headResponse.headers["content-type"];

          if (
            mimeTypeFromHead &&
            mimeTypeFromHead !== "application/octet-stream"
          ) {
            const inferred = mime.extension(mimeTypeFromHead);
            if (inferred) {
              fileExtension = inferred.toLowerCase();
              console.log(
                `📦 Inferred from HEAD content-type: ${fileExtension}`
              );
            } else {
              console.warn(
                `⚠️ MIME type detected but could not infer extension: ${mimeTypeFromHead}`
              );
            }
          } else {
            console.warn(
              `⚠️ HEAD response returned generic MIME type: ${mimeTypeFromHead}`
            );
          }
        } catch (err) {
          console.warn(
            `⚠️ Failed to infer file extension via HEAD:`,
            err instanceof Error ? err.message : err
          );
        }
      }

      /*
      if (!fileExtension || fileExtension === 'bin') {
        const mimeTypeFromHead = await axios.head(documentUrl, { timeout: 5000 }).then(
          (res) => res.headers['content-type'],
          () => null
        );
      
        if (mimeTypeFromHead) {
          const inferred = mime.extension(mimeTypeFromHead);
          if (inferred) {
            fileExtension = inferred.toLowerCase();
            console.log(`📦 Inferred from HEAD content-type: ${fileExtension}`);
          }
        }
      }
     */

      // 🛡️ Final fallback
      // 🛡️ Final fallback — only override if there's no valid extension
      if (!fileExtension || !SUPPORTED_FILE_TYPES.includes(fileExtension)) {
        console.warn(
          `⚠️ Could not detect or unsupported file extension: ${fileExtension}`
        );

        const extFromUrl = path
          .extname(new URL(documentUrl).pathname)
          .slice(1)
          .toLowerCase();
        if (SUPPORTED_FILE_TYPES.includes(extFromUrl)) {
          fileExtension = extFromUrl;
          console.log(
            `✅ Recovered valid extension from URL: ${fileExtension}`
          );
        } else {
          fileExtension = "pdf"; // Safe default
          console.log(`⚠️ Defaulting to fallback extension: pdf`);
        }
      }

      /*
      if (!fileExtension || !SUPPORTED_FILE_TYPES.includes(fileExtension)) {
        console.warn(`⚠️ Could not detect file extension. Falling back to 'pdf'.`);
        fileExtension = 'pdf';
      }*/

      console.log(`✅ Final fileExtension resolved: [${fileExtension}]`);

      // Step 5: Upload Icon if present
      if (record.cont_thumb) {
        const iconUploaded = await this.uploadIcon(
          record.cont_thumb,
          userToken,
          userId
        );
        iconUrl = iconUploaded.result?.content_url || "";
        console.log("Icon URL:", iconUrl);
      }

      console.log(`Resolved file extension: ${fileExtension}`);
      // Step 1: Create Content
      let mimeType = isYouTubeURL
        ? "video/x-youtube"
        : fileExtension === "zip"
        ? "application/vnd.ekstep.html-archive"
        : mime.lookup(fileExtension) || "application/octet-stream";

      if (fileExtension == "mp4") {
        mimeType = "video/mp4"; // Ensure correct MIME type for MP4 files
      }

      const payload = {
        request: {
          content: {
            name: title,
            code: uniqueCode,
            mimeType: mimeType,
            primaryCategory: primaryCategory,
            framework: this.framework,
            createdBy: userId || "",
            appIcon: iconUrl || "", // Use iconUrl if available
            ...additionalFields, // Merged dynamic fields here
          },
        },
      };

      const payloadString = JSON.stringify(payload);
      const contentLength = Buffer.byteLength(payloadString, "utf8");

      const headers = {
        "Content-Type": "application/json",
        "Content-Length": contentLength,
        tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
        Authorization: `Bearer ${userToken}`,
        "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
      };

      // console.log(payloadString);
      // console.log(headers);
      // console.log(`${this.middlewareUrl}/action/content/v3/create`);

      /*
      const createResponse = await axios.post(
        `${this.middlewareUrl}/action/content/v3/create`,
        payload,
        { headers }
      );
      */
      const createResponse = await this.retryRequest(
        () =>
          axios.post(
            `${this.middlewareUrl}/action/content/v3/create`,
            payload,
            { headers }
          ),
        3,
        2000,
        "Create Content"
      );

      // console.log(createResponse);

      const { identifier: doId, versionKey } = createResponse.data.result;
      console.log("Content created:", { doId, versionKey });

      // Step 1: Check if it's a YouTube URL
      if (
        /^(https?:\/\/)?(www\.)?(youtube\.com|youtu\.be)\/.+$/.test(documentUrl)
      ) {
        console.log(
          "YouTube URL detected, skipping file download and S3 upload."
        );
        fileUrl = documentUrl;
      } else {
        console.log("In s3 upload.");

        // const fileExtension = path.extname(new URL(documentUrl).pathname).slice(1);

        console.log(
          `Final fileExtension resolved for validation: [${fileExtension}]`
        );

        if (!SUPPORTED_FILE_TYPES.includes(fileExtension)) {
          console.log("Gracefully exit without throwing");
          return null; // Gracefully exit without throwing
        }

        // Step 2: Download Document
        const agent = new https.Agent({
          rejectUnauthorized: false, // ⚠️ Disable SSL certificate validation
        });
        /*
        const documentResponse = await axios.get(documentUrl, { 
          responseType: 'stream', 
          httpsAgent: agent,
          headers: {} // Ensure no unnecessary headers are passed
        });

        tempFilePath = `/tmp/${uniqueCode}.${fileExtension}`;
        const writer = fs.createWriteStream(tempFilePath);
        documentResponse.data.pipe(writer);
        */
        const documentResponse =
          await this.fetchWithRetries<NodeJS.ReadableStream>(documentUrl, {
            responseType: "stream",
            httpsAgent: agent,
            headers: {},
          });

        tempFilePath = `/tmp/${uniqueCode}.${fileExtension}`;
        const writer = fs.createWriteStream(tempFilePath);
        (documentResponse.data as NodeJS.ReadableStream).pipe(writer);

        await new Promise((resolve, reject) => {
          writer.on("finish", resolve);
          writer.on("error", reject);
        });

        // === ZIP FILE LOGIC ===
        if (fileExtension === "zip") {
          console.log(
            `🚀 Starting optimized ZIP processing for: ${documentUrl}`
          );

          try {
            // Use optimized streaming ZIP processing
            const JSZip = require("jszip");

            // Download ZIP file directly from URL
            const agent = new https.Agent({ rejectUnauthorized: false });
            const response = await axios.get(documentUrl, {
              responseType: "arraybuffer",
              httpsAgent: agent,
              timeout: 300000, // 5 minutes timeout
            });

            // console.log(`📦 ZIP downloaded, size: ${(response.data.length / 1024 / 1024).toFixed(2)} MB`);

            // Load ZIP with JSZip
            const zip = new JSZip();
            const zipData = await zip.loadAsync(response.data);

            // console.log(`📂 Extracting ZIP contents...`);

            // Get all files from ZIP
            const files = Object.keys(zipData.files);
            // console.log(`📁 Found ${files.length} files in ZIP`);

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
                  // console.log(`🔄 Detected nested folder structure, will flatten: ${rootFolder}`);
                }
              }
            }

            // Create new ZIP with JSZip
            const newZip = new JSZip();

            // Process each file
            for (const filePath of files) {
              if (zipData.files[filePath].dir) continue; // Skip directories

              const fileData = await zipData.files[filePath].async(
                "uint8array"
              );
              let newPath = filePath;

              // Flatten if needed
              if (needsFlattening && filePath.startsWith(rootFolder + "/")) {
                newPath = filePath.substring(rootFolder.length + 1);
              }

              newZip.file(newPath, fileData);
            }

            // console.log(`📦 Re-zipping with optimized settings...`);

            // Generate optimized ZIP stream
            const zipBuffer = await newZip.generateAsync({
              type: "nodebuffer",
              compression: "DEFLATE",
              compressionOptions: {
                level: 1, // Fast compression instead of level 9
              },
            });

            // Create temporary file for S3 upload
            const finalZipPath = `/tmp/${uniqueCode}_cleaned.zip`;
            fs.writeFileSync(finalZipPath, zipBuffer);

            // Replace tempFilePath with the cleaned ZIP path for S3 upload
            tempFilePath = finalZipPath;

            // console.log(`✅ Optimized ZIP processing completed`);
          } catch (error) {
            console.error(`❌ Optimized ZIP processing failed:`, error);
            throw error;
          }
        }

        // === AWS S3 Upload Logic ===
        // Use streaming upload for MP4 and M4V files
        if (fileExtension === "mp4" || fileExtension === "m4v") {
          // Always upload as .mp4
          const s3Extension = "mp4";
          console.log("🎬 Using streaming upload for MP4/M4V file");
          try {
            fileUrl = await this.streamUploadToS3(
              documentUrl,
              doId,
              s3Extension,
              "video/mp4"
            );
            console.log("✅ MP4/M4V stream upload completed:", fileUrl);
          } catch (streamError) {
            console.warn(
              "⚠️ Stream upload failed, falling back to temp file upload:",
              streamError
            );
            // Fall back to original method
            await this.uploadViaTempFile(
              tempFilePath,
              doId,
              s3Extension,
              "video/mp4"
            );
            fileUrl = `https://${process.env.AWS_BUCKET_NAME}.s3-${process.env.AWS_REGION}.amazonaws.com/content/assets/${doId}/file.${s3Extension}`;
          }
        } else {
          // Use optimized temp file method for other file types (including ZIP)
          await this.uploadViaTempFile(
            tempFilePath,
            doId,
            fileExtension,
            mimeType
          );
          fileUrl = `https://${process.env.AWS_BUCKET_NAME}.s3-${process.env.AWS_REGION}.amazonaws.com/content/assets/${doId}/file.${fileExtension}`;
        }

        console.log("fileUrl:", fileUrl);

        // Clean up temporary files
        if (tempFilePath && fs.existsSync(tempFilePath)) {
          fs.unlinkSync(tempFilePath);
        }
      }

      // Step 3: Return Response with temp file path for reuse
      return { doId, versionKey, fileUrl, tempFilePath };
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      const logMessage = `❌ Failed to create content record with documentUrl: ${documentUrl} - ${errorMessage}`;

      // ✅ Print error in console
      console.error(logMessage);

      // ✅ Log error to file
      this.logErrorToFile(logMessage);

      return;
    } finally {
      // Always clean up temporary files if they exist
      if (tempFilePath && fs.existsSync(tempFilePath)) {
        try {
          fs.unlinkSync(tempFilePath);
        } catch (cleanupErr) {
          console.warn(
            `⚠️ Failed to delete temp file ${tempFilePath}:`,
            cleanupErr
          );
        }
      }
    }
  }

  /**
   * Stream upload directly from URL to S3 without downloading to temp files
   * This is more efficient for large files like MP4
   */
  private async streamUploadToS3(
    documentUrl: string,
    doId: string,
    fileExtension: string,
    mimeType: string
  ): Promise<string> {
    try {
      console.log(`🚀 Starting stream upload for: ${documentUrl}`);
      console.log(`📁 S3 Key: content/assets/${doId}/file.${fileExtension}`);
      console.log(`🎯 MIME Type: ${mimeType}`);

      // Configure AWS S3
      AWS.config.update({
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        region: process.env.AWS_REGION,
      });

      const s3 = new AWS.S3();
      const bucketName = process.env.AWS_BUCKET_NAME || "";
      const s3Key = `content/assets/${doId}/file.${fileExtension}`;

      console.log(`🏪 S3 Bucket: ${bucketName}`);
      console.log(`🌍 AWS Region: ${process.env.AWS_REGION}`);

      // Check if file already exists in S3
      try {
        console.log(`🔍 Checking if file already exists in S3...`);
        await s3.headObject({ Bucket: bucketName, Key: s3Key }).promise();
        console.log(`✅ File already exists in S3, skipping upload`);
        const fileUrl = `https://${bucketName}.s3-${AWS.config.region}.amazonaws.com/${s3Key}`;
        console.log("🔗 Existing URL:", fileUrl);
        return fileUrl;
      } catch (headError: any) {
        if (headError.code === "NotFound") {
          console.log(`📝 File not found in S3, proceeding with upload`);
        } else {
          console.warn(
            `⚠️ Error checking S3 file existence: ${headError.message}`
          );
        }
      }

      // Get file stream from URL
      console.log(`📡 Fetching stream from URL...`);
      const agent = new https.Agent({
        rejectUnauthorized: false,
      });

      const documentResponse =
        await this.fetchWithRetries<NodeJS.ReadableStream>(documentUrl, {
          responseType: "stream",
          httpsAgent: agent,
          headers: {},
        });

      const fileStream = documentResponse.data as NodeJS.ReadableStream;
      console.log(`✅ Stream obtained successfully`);

      // Add progress tracking to the stream
      let uploadedBytes = 0;
      let lastLoggedMB = 0;
      const progressStream = new (require("stream").Transform)({
        transform(chunk: any, encoding: any, callback: any) {
          uploadedBytes += chunk.length;
          const currentMB = Math.floor(uploadedBytes / (1024 * 1024));

          // Only log when we reach a new 10MB increment
          if (currentMB >= lastLoggedMB + 10) {
            lastLoggedMB = currentMB;
          }

          callback(null, chunk);
        },
      });

      fileStream.pipe(progressStream);

      console.log(`🚀 Starting S3 upload with streaming...`);
      console.log(`⚙️  Chunk size: 10MB, Queue size: 4`);

      // Upload directly to S3 using managed upload with streaming
      const uploadResponse = await this.retryRequest(
        () =>
          s3
            .upload(
              {
                Bucket: bucketName,
                Key: s3Key,
                Body: progressStream,
                ContentType: mimeType || "application/octet-stream",
              },
              {
                // Configure chunked upload behavior
                partSize: 10 * 1024 * 1024, // 10 MB chunks
                queueSize: 4,
              }
            )
            .promise(),
        3,
        2000,
        "S3 Stream Upload"
      );

      console.log("✅ Stream upload successful!");
      console.log("📍 S3 Location:", uploadResponse.Location);
      console.log("🆔 ETag:", uploadResponse.ETag);

      // Generate the S3 URL
      const fileUrl = `https://${bucketName}.s3-${AWS.config.region}.amazonaws.com/${s3Key}`;
      console.log("🔗 Generated URL:", fileUrl);

      return fileUrl;
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      console.error(
        `❌ Stream upload failed for ${documentUrl}: ${errorMessage}`
      );
      console.error(`🔍 Error details:`, error);
      throw error;
    }
  }

  /**
   * Stream-compress an MP4 from URL and upload to S3 (default compression, no size targeting)
   */
  /*
  private async streamCompressAndUploadMp4ToS3(
    documentUrl: string,
    doId: string,
    fileExtension: string,
    mimeType: string
  ): Promise<string> {
    const ffmpeg = require('fluent-ffmpeg');
    const AWS = require('aws-sdk');
    const axios = require('axios');
    const stream = require('stream');
    const https = require('https');

    AWS.config.update({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION,
    });
    const s3 = new AWS.S3();
    const bucketName = process.env.AWS_BUCKET_NAME || "";
    const s3Key = `content/assets/${doId}/file.mp4`;

    // Check if file already exists in S3
    try {
      await s3.headObject({ Bucket: bucketName, Key: s3Key }).promise();
      console.log(`✅ File already exists in S3, skipping upload`);
      const fileUrl = `https://${bucketName}.s3-${AWS.config.region}.amazonaws.com/${s3Key}`;
      return fileUrl;
    } catch (headError: any) {
      if (headError.code === 'NotFound') {
        console.log(`📝 File not found in S3, proceeding with upload`);
      } else {
        console.warn(`⚠️ Error checking S3 file existence: ${headError.message}`);
      }
    }

    // Get file stream from URL
    const agent = new https.Agent({ rejectUnauthorized: false });
    const documentResponse = await axios.get(documentUrl, { responseType: 'stream', httpsAgent: agent });
    const inputStream = documentResponse.data;

    // Set up ffmpeg pipeline with default compression
    const ffmpegStream = ffmpeg(inputStream)
      .videoCodec('libx264')
      .audioCodec('aac')
      .outputOptions([
        '-preset fast',
        '-crf 28',
        '-movflags +faststart',
        '-max_muxing_queue_size 1024'
      ])
      .format('mp4')
      .on('start', (cmd: string) => console.log('FFmpeg started:', cmd))
      .on('progress', (progress: any) => {
        if (progress.targetSize) {
          console.log(`FFmpeg progress: ${progress.targetSize} KB`);
        }
      })
      .on('error', (err: any) => console.error('FFmpeg error:', err.message))
      .on('end', () => console.log('FFmpeg finished'))
      .pipe();

    // Progress tracking for S3 upload
    let uploadedBytes = 0;
    let lastLoggedMB = 0;
    const progressStream = new stream.Transform({
      transform(chunk: any, encoding: any, callback: any) {
        uploadedBytes += chunk.length;
        const currentMB = Math.floor(uploadedBytes / (1024 * 1024));
        if (currentMB >= lastLoggedMB + 5) {
          lastLoggedMB = currentMB;
          console.log(`📊 Uploaded: ${currentMB} MB`);
        }
        callback(null, chunk);
      }
    });
    ffmpegStream.pipe(progressStream);

    // Upload to S3
    const uploadResponse = await s3.upload({
      Bucket: bucketName,
      Key: s3Key,
      Body: progressStream,
      ContentType: 'video/mp4',
      partSize: 5 * 1024 * 1024,
      queueSize: 2
    }).promise();

    console.log('✅ Compressed stream upload successful:', uploadResponse.Location);
    const fileUrl = `https://${bucketName}.s3-${AWS.config.region}.amazonaws.com/${s3Key}`;
    return fileUrl;
  }
  */

  /**
   * Upload file to S3 using temp file (original method for fallback)
   */
  private async uploadViaTempFile(
    tempFilePath: string | null,
    doId: string,
    fileExtension: string,
    mimeType: string
  ): Promise<void> {
    if (!tempFilePath) {
      throw new Error("tempFilePath is null. Cannot upload to S3.");
    }

    // Configure AWS S3
    AWS.config.update({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION,
    });

    const s3 = new AWS.S3();
    const bucketName = process.env.AWS_BUCKET_NAME || "";
    const s3Key = `content/assets/${doId}/file.${fileExtension}`;

    // Check if file already exists in S3
    try {
      console.log(`🔍 Checking if file already exists in S3 (temp method)...`);
      await s3.headObject({ Bucket: bucketName, Key: s3Key }).promise();
      console.log(`✅ File already exists in S3, skipping temp file upload`);
      return;
    } catch (headError: any) {
      if (headError.code === "NotFound") {
        console.log(
          `📝 File not found in S3, proceeding with temp file upload`
        );
      } else {
        console.warn(
          `⚠️ Error checking S3 file existence: ${headError.message}`
        );
      }
    }

    const uploadResponse = await this.retryRequest(
      () =>
        s3
          .upload(
            {
              Bucket: bucketName,
              Key: s3Key,
              Body: fs.createReadStream(tempFilePath),
              ContentType: mimeType || "application/octet-stream",
            },
            {
              // Configure chunked upload behavior
              partSize: 5 * 1024 * 1024, // 5 MB chunks (updated to match streaming)
              queueSize: 4,
            }
          )
          .promise(),
      3,
      2000,
      "S3 Temp File Upload"
    );

    console.log("✅ Temp file upload successful:", uploadResponse.Location);
  }

  /**
   * Stream-process ZIP file from URL directly to S3 (optimized)
   */
  private async streamProcessZipToS3(
    documentUrl: string,
    doId: string,
    fileExtension: string,
    mimeType: string
  ): Promise<string> {
    const yauzl = require("yauzl");
    const JSZip = require("jszip");
    const AWS = require("aws-sdk");
    const axios = require("axios");
    const stream = require("stream");
    const https = require("https");
    const { promisify } = require("util");

    AWS.config.update({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.AWS_REGION,
    });
    const s3 = new AWS.S3();
    const bucketName = process.env.AWS_BUCKET_NAME || "";
    const s3Key = `content/assets/${doId}/file.${fileExtension}`;

    // Check if file already exists in S3
    try {
      await s3.headObject({ Bucket: bucketName, Key: s3Key }).promise();
      console.log(`✅ ZIP file already exists in S3, skipping upload`);
      const fileUrl = `https://${bucketName}.s3-${AWS.config.region}.amazonaws.com/${s3Key}`;
      return fileUrl;
    } catch (headError: any) {
      if (headError.code === "NotFound") {
        console.log(
          `📝 ZIP file not found in S3, proceeding with stream processing`
        );
      } else {
        console.warn(
          `⚠️ Error checking S3 file existence: ${headError.message}`
        );
      }
    }

    console.log(`🚀 Starting streaming ZIP processing for: ${documentUrl}`);

    try {
      // Get ZIP stream from URL
      const agent = new https.Agent({ rejectUnauthorized: false });
      const response = await axios.get(documentUrl, {
        responseType: "arraybuffer",
        httpsAgent: agent,
        timeout: 300000, // 5 minutes timeout
      });

      // console.log(`📦 ZIP downloaded, size: ${(response.data.length / 1024 / 1024).toFixed(2)} MB`);

      // Load ZIP with JSZip
      const zip = new JSZip();
      const zipData = await zip.loadAsync(response.data);

      // console.log(`📂 Extracting ZIP contents...`);

      // Get all files from ZIP
      const files = Object.keys(zipData.files);
      // console.log(`📁 Found ${files.length} files in ZIP`);

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
            // console.log(`🔄 Detected nested folder structure, will flatten: ${rootFolder}`);
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

      // console.log(`📦 Re-zipping with optimized settings...`);

      // Generate optimized ZIP stream
      const zipStream = newZip.generateAsync({
        type: "nodebuffer",
        compression: "DEFLATE",
        compressionOptions: {
          level: 1, // Fast compression instead of level 9
        },
      });

      // Progress tracking for S3 upload
      let uploadedBytes = 0;
      let lastLoggedMB = 0;
      const progressStream = new stream.Transform({
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

      // Create readable stream from buffer
      const bufferStream = new stream.Readable();
      bufferStream.push(zipStream);
      bufferStream.push(null);
      bufferStream.pipe(progressStream);

      console.log(`🚀 Starting S3 upload with optimized ZIP...`);
      console.log(`⚙️  Chunk size: 5MB, Queue size: 4`);

      // Upload to S3
      const uploadResponse = await s3
        .upload(
          {
            Bucket: bucketName,
            Key: s3Key,
            Body: progressStream,
            ContentType: mimeType || "application/zip",
          },
          {
            partSize: 5 * 1024 * 1024,
            queueSize: 4,
          }
        )
        .promise();

      console.log(
        "✅ Optimized ZIP stream upload successful:",
        uploadResponse.Location
      );
      const fileUrl = `https://${bucketName}.s3-${AWS.config.region}.amazonaws.com/${s3Key}`;
      return fileUrl;
    } catch (error) {
      console.error(`❌ Stream ZIP processing failed:`, error);
      throw error;
    }
  }

  private logErrorToFile(logMessage: string): void {
    const logFilePath = path.join(process.cwd(), "error.log"); // Ensures log is in a fixed location

    // ✅ Write log to `error.log`
    fs.appendFile(
      logFilePath,
      `${new Date().toISOString()} - ${logMessage}\n`,
      (err) => {
        if (err) console.error("❌ Failed to write to error.log", err);
      }
    );
  }

  private async fetchWithRetries<T>(
    url: string,
    options: any = {},
    retries = 3,
    delayMs = 2000
  ): Promise<AxiosResponse<T>> {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const response = await axios.get<T>(url, options);
        return response; // Return full AxiosResponse<T>
      } catch (error: any) {
        const message = error?.code || error?.message || error.toString();

        if (
          message.includes("EAI_AGAIN") ||
          message.includes("ENOTFOUND") ||
          message.includes("ETIMEDOUT")
        ) {
          console.warn(
            `⚠️ Attempt ${attempt} failed with network error: ${message}`
          );
          if (attempt < retries) {
            await new Promise((resolve) => setTimeout(resolve, delayMs));
            continue;
          }
        }

        throw error;
      }
    }

    throw new Error(`Failed to fetch ${url} after ${retries} attempts`);
  }

  private async validateFileUrl(
    fileUrl: string,
    record: Content
  ): Promise<boolean> {
    // const SUPPORTED_FILE_TYPES = ["pdf", "mp4", "zip", "mp3"];

    // Added for m4v support
    const SUPPORTED_FILE_TYPES = ["pdf", "mp4", "zip", "mp3", "m4v"];

    const isYouTubeUrl =
      /^(https?:\/\/)?(www\.)?(youtube\.com|youtu\.be)\//.test(fileUrl);
    const isGoogleDriveUrl = /drive\.google\.com\/file\/d\/([^/]+)\//.test(
      fileUrl
    );

    if (isYouTubeUrl) {
      console.log(`Skipping file existence check for YouTube URL: ${fileUrl}`);
      return true;
    }

    if (isGoogleDriveUrl) {
      console.log(
        `Skipping file existence check for Google Drive URL: ${fileUrl}`
      );
      return true;
    }

    const ext = path.extname(new URL(fileUrl).pathname).slice(1).toLowerCase();

    try {
      let response;
      if (ext === "zip" || ext === "pdf") {
        const agent = new https.Agent({
          rejectUnauthorized: false, // ⛔ WARNING: disables SSL verification
        });

        // Primary check using HEAD request
        response = await axios.head(fileUrl, {
          timeout: 15000,
          httpsAgent: agent,
        });
      } else {
        response = await axios.head(fileUrl, { timeout: 15000 });
      }

      if (response.status !== 200) {
        throw new Error(`Unexpected status code: ${response.status}`);
      }

      const mimeType = response.headers["content-type"];
      console.log(`File exists: ${fileUrl} (MIME: ${mimeType}, EXT: ${ext})`);

      if (!SUPPORTED_FILE_TYPES.includes(ext)) {
        throw new Error(`Unsupported file type: ${ext} for URL: ${fileUrl}`);
      }

      return true;
    } catch (headError) {
      console.warn(
        `HEAD request failed for ${fileUrl}. Attempting GET fallback...`
      );

      try {
        // Fallback check using GET request with Range header (fetch only 1st byte)
        const response = await axios.get(fileUrl, {
          headers: { Range: "bytes=0-0" },
          timeout: 15000,
        });

        const mimeType = response.headers["content-type"];
        console.log(
          `(Fallback) File exists: ${fileUrl} (MIME: ${mimeType}, EXT: ${ext})`
        );

        if (!SUPPORTED_FILE_TYPES.includes(ext)) {
          throw new Error(`Unsupported file type: ${ext} for URL: ${fileUrl}`);
        }

        return true;
      } catch (getError) {
        const errorMessage =
          getError instanceof Error
            ? getError.message
            : "Unknown fallback error";
        const logMessage = `❌ Fallback failed: ${fileUrl}. Title: ${record.cont_title} - ${errorMessage}`;

        console.error(logMessage);
        this.logErrorToFile(logMessage);
        return false;
      }
    }
  }

  private async uploadContent(
    contentId: string,
    fileUrl: string,
    userToken: string
  ) {
    try {
      console.log("uploadContent");

      const YOUTUBE_URL_REGEX =
        /^(https?:\/\/)?(www\.)?(youtube\.com|youtu\.be)\/.+$/;

      // Check if the URL is a YouTube URL
      const isYouTubeURL = YOUTUBE_URL_REGEX.test(fileUrl);

      const path = require("path");
      const mime = require("mime-types"); // Ensure this library is installed

      let mimeType: string | false = false;
      let tempFilePath: string | null = null;
      fileUrl = fileUrl.trim(); // ✅ Trim spaces

      // Step 3: Prepare FormData and Payload
      const formData = new FormData();

      if (isYouTubeURL) {
        this.logger.log(
          "YouTube URL detected, skipping file download and S3 upload."
        );
        mimeType = "video/x-youtube";
        formData.append("fileUrl", fileUrl);
        formData.append("mimeType", mimeType); // ✅ Added missing field
      } else {
        const fileExtension = path.extname(new URL(fileUrl).pathname).slice(1); // e.g., 'pdf'

        mimeType =
          fileExtension === "zip"
            ? "application/vnd.ekstep.html-archive"
            : mime.lookup(fileExtension) || "application/octet-stream";

        if (fileExtension == "mp4") {
          mimeType = "video/mp4"; // Ensure correct MIME type for MP4 files
        }

        formData.append("fileUrl", fileUrl);
        formData.append("mimeType", mimeType);

        // Step 2: Download the file dynamically with its correct extension
        tempFilePath = await this.downloadFileToTemp(
          fileUrl,
          `upload_${Date.now()}.${fileExtension}`
        );
        formData.append("file", fs.createReadStream(tempFilePath));
      }

      // Step 1: Prepare headers
      const headers = {
        tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
        Authorization: `Bearer ${userToken}`,
        "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
        ...formData.getHeaders(),
      };

      // console.log('isYouTubeURL');
      // console.log(fileUrl);

      // Step 2: Prepare payload
      const payload = {
        request: {
          content: {
            fileUrl: fileUrl,
            mimeType: mimeType,
          },
        },
      };

      // Step 6: Upload to Middleware
      const uploadUrl = `${this.middlewareUrl}/action/content/v3/upload/${contentId}`;
      // console.log('Upload URL:', uploadUrl);

      // console.log(payload);
      // console.log(formData);
      // console.log(headers);
      console.log(uploadUrl);

      // Check if we have a file to upload (not YouTube URL)
      const hasFile = !isYouTubeURL && tempFilePath;
      let uploadFileResponse: any;
      if (
        hasFile &&
        tempFilePath &&
        mimeType != "application/vnd.ekstep.html-archive"
      ) {
        const fileSizeMB = fs.statSync(tempFilePath).size / (1024 * 1024);
        if (fileSizeMB > 50) {
          // Use the new multipart upload logic for files > 99MB
          const fileName = path.basename(tempFilePath);
          uploadFileResponse = await this.multipartUploadToSunbird(
            contentId,
            tempFilePath,
            fileName
          );
          // console.log(uploadFileResponse);
        } else {
          // Use the normal upload logic for files <= 99MB
          uploadFileResponse = await axios.post(uploadUrl, formData, {
            headers,
          });
          // console.log(uploadFileResponse);
        }
      } else {
        // ... existing logic for YouTube URL or other case ...
        uploadFileResponse = await axios.post(uploadUrl, formData, {
          headers,
        });
        // console.log(uploadFileResponse);
      }

      if (
        this.isZipFile(fileUrl) &&
        uploadFileResponse.status === 500 &&
        !uploadFileResponse.data.success
      ) {
        console.log("Initial upload failed. Retrying...");
        const retryResult = await this.retryUntilSuccess(fileUrl);
        if (tempFilePath) {
          fs.unlinkSync(tempFilePath);
        }
        return retryResult;
      }

      // console.log('File Upload Response:', uploadFileResponse.data);

      // Clean up temporary file if it exists
      if (tempFilePath) {
        fs.unlinkSync(tempFilePath);
      }

      return uploadFileResponse.data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error("Error during file upload (Axios):", error.message);
        if (this.isZipFile(fileUrl) && error.response?.status === 500) {
          console.log("Retrying due to server error...");
          return await this.retryUntilSuccess(fileUrl); // <-- Return the retry result
        }
      } else if (error instanceof Error) {
        console.error("Error during file upload (Generic):", error.message);
      } else {
        console.error("Unknown error during file upload:", error);
      }
    }
  }

  private async retryUntilSuccess(contentUrl: any): Promise<any> {
    if (!this.isZipFile(contentUrl)) {
      console.error(
        `Invalid file type: Only .zip files are supported. Provided URL: ${contentUrl}`
      );
      return;
    }

    let success = false;
    let retries = 0;
    const startTime = Date.now();
    let uploadUrl = `${this.frontendURL}/api/content-upload/get-status`;
    let userToken = this.configService.get<string>("USER_TOKEN") || "";

    const headers = {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: `Bearer ${userToken}`,
    };

    // Generate formData dynamically
    const formData = {
      contenturl: contentUrl,
    };

    while (!success) {
      retries++;
      try {
        const response = await axios.post(uploadUrl, formData, {
          headers: headers,
        });
        const data = response.data;
        console.log(`Retry ${retries}: Response -`, data);

        if (data.success) {
          success = true;
          const endTime = Date.now();
          const timeTaken = (endTime - startTime) / 1000; // Time in seconds

          console.log(
            `Operation succeeded after ${retries} retries and ${timeTaken} seconds.`
          );
          this.logToFile(retries, timeTaken);
          return data; // Return the successful data
        } else {
          console.log(
            `Retry ${retries}: Current status - success: ${data.success}`
          );
        }
      } catch (error) {
        if (axios.isAxiosError(error)) {
          console.error(
            `Retry ${retries}: Axios error occurred -`,
            error.message
          );
        } else if (error instanceof Error) {
          console.error(
            `Retry ${retries}: Generic error occurred -`,
            error.message
          );
        } else {
          console.error(`Retry ${retries}: Unknown error occurred -`, error);
        }
      }

      // Wait for 2 seconds before retrying
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }

  private isZipFile(fileUrl: string): boolean {
    return fileUrl.toLowerCase().endsWith(".zip");
  }

  private logToFile(retries: number, timeTaken: number) {
    const logMessage = `Success after ${retries} retries and ${timeTaken} seconds.\n`;
    const logFilePath = "upload_log.txt";

    fs.appendFile(logFilePath, logMessage, (err) => {
      if (err) {
        console.error("Error writing to log file:", err.message);
      } else {
        console.log("Log written to file:", logFilePath);
      }
    });
  }

  private async downloadFileToTemp(
    fileUrl: string,
    fileName: string
  ): Promise<string> {
    const tempFilePath = path.join("/tmp", fileName);
    try {
      const response = await axios.get(fileUrl, { responseType: "stream" });
      const writer = fs.createWriteStream(tempFilePath);
      response.data.pipe(writer);

      await new Promise((resolve, reject) => {
        writer.on("finish", resolve);
        writer.on("error", reject);
      });

      // console.log(`File downloaded to: ${tempFilePath}`);
      return tempFilePath;
    } catch (error) {
      if (error instanceof Error) {
        console.error(`Error downloading file from ${fileUrl}:`, error.message);
      } else {
        console.error(
          `Unexpected error while downloading file from ${fileUrl}:`,
          error
        );
      }
      // Re-throw the error to ensure the function does not return undefined
      throw error;
    }
  }

  private async reviewContent(contentId: string, userToken: string) {
    try {
      const headers = this.getHeaders(userToken);
      const reviewUrl = this.getUrl(`/action/content/v3/review/${contentId}`);

      console.log("Calling reviewContent API:", reviewUrl);

      //const response = await axios.post(reviewUrl, {}, { headers });
      const response = await this.retryRequest(
        () => axios.post(reviewUrl, {}, { headers }),
        3,
        2000,
        "reviewContent"
      );

      // console.log("Review API Response:", response.data);

      return response.data;
    } catch (error) {
      this.handleApiError("reviewContent", error, contentId);
    }
  }

  private handleApiError(
    methodName: string,
    error: unknown,
    contentId?: string
  ) {
    const errorMessage =
      error instanceof Error ? error.message : "Unknown error";
    const logMessage =
      `❌ API Error in ${methodName}: ${errorMessage}` +
      (contentId ? ` (Content ID: ${contentId})` : "");

    // ✅ Print error in console for debugging
    console.error(logMessage);

    // ✅ Log error to file
    this.logErrorToFile(logMessage);
  }

  private async publishContent(contentId: string, userToken: string) {
    try {
      const headers = this.getHeaders(userToken);
      const publishUrl = this.getUrl(`/action/content/v3/publish/${contentId}`);
      const userId = this.configService.get<string>("USER_ID") || "";

      console.log("Calling publishContent API:", publishUrl);

      const body = {
        request: {
          content: {
            publishChecklist: [
              "No Hate speech, Abuse, Violence, Profanity",
              "Is suitable for children",
              "Correct Board, Grade, Subject, Medium",
              "Appropriate Title, Description",
              "No Sexual content, Nudity or Vulgarity",
              "No Discrimination or Defamation",
              "Appropriate tags such as Resource Type, Concepts",
              "Relevant Keywords",
              "Audio (if any) is clear and easy to understand",
              "No Spelling mistakes in the text",
              "Language is simple to understand",
              "Can see the content clearly on Desktop and App",
              "Content plays correctly",
            ],
            lastPublishedBy: userId,
          },
        },
      };

      // console.log("Publish API Response:", body);

      // const response = await axios.post(publishUrl, body, { headers });
      const response = await this.retryRequest(
        () => axios.post(publishUrl, body, { headers }),
        3,
        2000,
        "publishContent"
      );

      // console.log("Publish API Response:", response.data);

      return response.data;
    } catch (error) {
      this.handleApiError("publishContent", error, contentId);
    }
  }

  private async retryRequest<T>(
    fn: () => Promise<T>,
    retries = 3,
    delayMs = 2000,
    label = "API"
  ): Promise<T> {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const result = await fn();
        return result;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(`⚠️ ${label} attempt ${attempt} failed: ${message}`);
        if (attempt < retries) {
          await new Promise((res) => setTimeout(res, delayMs));
        } else {
          this.handleApiError(label, error);
          throw error;
        }
      }
    }
    throw new Error(`${label} failed after ${retries} retries`);
  }

  private async uploadFileInChunks(
    uploadUrl: string,
    formData: FormData,
    headers: any,
    tempFilePath: string,
    fileSize: number,
    chunkSize: number,
    fileUrl?: string,
    mimeType?: string
  ): Promise<any> {
    try {
      const totalChunks = Math.ceil(fileSize / chunkSize);
      console.log(
        `Uploading file in ${totalChunks} chunks of ${(
          chunkSize /
          1024 /
          1024
        ).toFixed(2)}MB each`
      );

      // For chunked upload, we'll use a different approach
      // Since the middleware might not support multipart chunked uploads,
      // we'll use a streaming approach with axios

      const fileStream = fs.createReadStream(tempFilePath);

      // Create a new FormData for streaming
      const streamFormData = new FormData();

      // Add the same fields as the original formData
      if (fileUrl) {
        streamFormData.append("fileUrl", fileUrl);
      }
      if (mimeType) {
        streamFormData.append("mimeType", mimeType);
      }

      // Add the file as a stream
      streamFormData.append("file", fileStream);

      // Update headers for streaming
      const streamHeaders = {
        ...headers,
        ...streamFormData.getHeaders(),
        "Content-Length": fileSize.toString(),
      };

      // Upload with streaming
      const response = await axios.post(uploadUrl, streamFormData, {
        headers: streamHeaders,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 300000, // 5 minutes timeout
        onUploadProgress: (progressEvent) => {
          if (progressEvent.total) {
            const percentCompleted = Math.round(
              (progressEvent.loaded * 100) / progressEvent.total
            );
            console.log(
              `Upload progress: ${percentCompleted}% (${(
                progressEvent.loaded /
                1024 /
                1024
              ).toFixed(2)}MB / ${(progressEvent.total / 1024 / 1024).toFixed(
                2
              )}MB)`
            );
          }
        },
      });

      return response;
    } catch (error) {
      console.error("Error in chunked upload:", error);
      throw error;
    }
  }

  // Add this new function for multipart upload to Sunbird
  private async multipartUploadToSunbird(
    contentId: string,
    tempFilePath: string,
    fileName: string
  ): Promise<any> {
    const MULTIPART_BASE_URL = `${this.configService.get<string>(
      "FRONTEND_URL"
    )}api/`;
    // "https://lap.prathamdigital.org/api/";
    const CHUNK_SIZE = 25 * 1024 * 1024; // 5MB
    const fileSize = fs.statSync(tempFilePath).size;
    const totalParts = Math.ceil(fileSize / CHUNK_SIZE);
    const fileParts: { PartNumber: number; ETag: string }[] = [];
    let partNumber = 1;
    let offset = 0;
    const fileBuffer = fs.readFileSync(tempFilePath);
    const userToken = this.configService.get<string>("USER_TOKEN");
    // Get MIME type
    const mime = require("mime-types");
    const mimeType = mime.lookup(fileName) || "application/octet-stream";
    // 1. Get uploadId from new endpoint
    const keyPath = `content/assets/${contentId}/${fileName}`;
    const getUploadIdUrl = `${MULTIPART_BASE_URL}multipart-upload/get-upload-id`;
    const getUploadIdHeaders = {
      Accept: "application/json, text/plain, */*",
      "Content-Type": "application/json",
      Authorization: `Bearer ${userToken}`,
      tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
      "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
    };
    const getUploadIdBody = {
      keyPath,
      type: mimeType,
    };
    const getUploadIdResp = await axios.post(getUploadIdUrl, getUploadIdBody, {
      headers: getUploadIdHeaders,
    });
    const uploadId =
      getUploadIdResp.data?.uploadId || getUploadIdResp.data?.result?.upload_id;
    if (!uploadId)
      throw new Error(
        "Failed to get uploadId from Sunbird multipart-upload/get-upload-id"
      );

    // Define upload part URL
    const uploadPartUrl = `${MULTIPART_BASE_URL}multipart-upload/upload-part-command`;

    // 2. Upload chunks with last-chunk coalescing if needed
    const MIN_LAST_CHUNK_SIZE = 1 * 1024 * 1024; // 1MB
    while (offset < fileSize) {
      let end = Math.min(offset + CHUNK_SIZE, fileSize);
      // Check if this is the second-to-last chunk and the last chunk is small
      if (end < fileSize) {
        let nextEnd = Math.min(end + CHUNK_SIZE, fileSize);
        let lastChunkSize = fileSize - end;
        if (nextEnd === fileSize && lastChunkSize < MIN_LAST_CHUNK_SIZE) {
          // Combine this chunk and the last chunk
          end = fileSize;
        }
      }
      const chunk = fileBuffer.slice(offset, end);
      const chunkHeaders = {
        Accept: "application/json, text/plain, */*",
        "Content-Type": "application/octet-stream",
        "Content-Length": chunk.length,
        ContentLength: chunk.length,
        Authorization: `Bearer ${userToken}`,
        tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
        "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
        Key: keyPath,
        UploadId: uploadId,
        PartNumber: partNumber,
      };
      let uploadResp;
      try {
        uploadResp = await axios.put(uploadPartUrl, chunk, {
          headers: chunkHeaders,
        });
      } catch (err) {
        if (
          err &&
          typeof err === "object" &&
          "response" in err &&
          err.response &&
          typeof err.response === "object" &&
          "data" in err.response
        ) {
          console.error(
            `❌ Error uploading part ${partNumber}:`,
            err.response.data
          );
        } else {
          console.error(`❌ Error uploading part ${partNumber}:`, err);
        }
        const errMsg =
          err &&
          typeof err === "object" &&
          "message" in err &&
          typeof err.message === "string"
            ? err.message
            : String(err);
        throw new Error(`Failed to upload part ${partNumber}: ${errMsg}`);
      }
      // Extract ETag from response
      const ETag =
        uploadResp.data?.response?.ETag ||
        uploadResp.headers["etag"] ||
        uploadResp.data?.ETag ||
        uploadResp.data?.etag;
      if (!ETag) {
        console.error("No ETag found in upload response:", uploadResp.data);
        throw new Error("No ETag found in upload response");
      }
      fileParts.push({ PartNumber: partNumber, ETag });
      offset = end;
      partNumber++;
    }
    // 3. Complete upload
    const completeUrl = `${MULTIPART_BASE_URL}multipart-upload/completed-multipart-upload`;
    const completeHeaders = {
      Accept: "application/json, text/plain, */*",
      "Content-Type": "application/json",
      Authorization: `Bearer ${userToken}`,
      tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
      "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
    };
    const completeBody = {
      Key: keyPath,
      UploadId: uploadId,
      Parts: fileParts,
    };
    let completeResp;
    try {
      completeResp = await axios.post(completeUrl, completeBody, {
        headers: completeHeaders,
      });
    } catch (err) {
      if (
        err &&
        typeof err === "object" &&
        "response" in err &&
        err.response &&
        typeof err.response === "object" &&
        "data" in err.response
      ) {
        console.error(
          "❌ Error completing multipart upload:",
          err.response.data
        );
      } else {
        console.error("❌ Error completing multipart upload:", err);
      }
      const errMsg =
        err &&
        typeof err === "object" &&
        "message" in err &&
        typeof err.message === "string"
          ? err.message
          : String(err);
      throw new Error(`Failed to complete multipart upload: ${errMsg}`);
    }
    // Extract file URL and doId for the next step
    const fileUrl =
      completeResp.data?.response?.Location || completeResp.data?.response?.Key;
    if (!fileUrl) {
      throw new Error(
        "No file URL returned from completed-multipart-upload response"
      );
    }
    // Ensure all required parameters are present
    if (!fileUrl || !mimeType || !userToken) {
      throw new Error(
        `Missing required parameter for Sunbird file registration: fileUrl=${fileUrl}, mimeType=${mimeType}, userToken=${userToken}`
      );
    }
    // Register the file with Sunbird
    await this.registerFileWithSunbird(contentId, fileUrl, mimeType, userToken);
    return completeResp;
  }

  // Add this function to perform the Sunbird content upload step after multipart upload
  private async registerFileWithSunbird(
    doId: string,
    fileUrl: string,
    mimeType: string,
    userToken: string
  ): Promise<any> {
    const uploadUrl = `${this.middlewareUrl}/action/content/v3/upload/${doId}`;
    const FormData = require("form-data");
    const form = new FormData();
    form.append("fileUrl", fileUrl);
    form.append("mimeType", mimeType);
    const headers = {
      ...form.getHeaders(),
      Authorization: `Bearer ${userToken}`,
      tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
      "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
    };
    try {
      const response = await axios.post(uploadUrl, form, { headers });
      // console.log('Sunbird content upload response:', response.data);
      return response.data;
    } catch (err) {
      if (
        err &&
        typeof err === "object" &&
        "response" in err &&
        err.response &&
        typeof err.response === "object" &&
        "data" in err.response
      ) {
        console.error("❌ Error in Sunbird content upload:", err.response.data);
      } else {
        console.error("❌ Error in Sunbird content upload:", err);
      }
      const errMsg =
        err &&
        typeof err === "object" &&
        "message" in err &&
        typeof err.message === "string"
          ? err.message
          : String(err);
      throw new Error(`Failed to register file with Sunbird: ${errMsg}`);
    }
  }

  async uploadIcon(iconUrl: string, userToken: string , userId:string): Promise<any> {
    const tempDir = "/tmp"; // Temporary directory to save the file
    let tempFilePath = "";
    let doId = "";

    try {
      const { v4: uuidv4 } = require("uuid");
      const path = require("path");
      const fs = require("fs");
      const FormData = require("form-data");
      const fileType = require("file-type");

      // Step 1: Download the image to a temp file first
      const response = await axios.get(iconUrl, { responseType: "stream" });
      const fileName =
        path.basename(iconUrl.split("?")[0]) || `${uuidv4()}.png`;
      tempFilePath = path.join(tempDir, fileName);

      const writer = fs.createWriteStream(tempFilePath);
      response.data.pipe(writer);
      await new Promise<void>((resolve, reject) => {
        writer.on("finish", resolve);
        writer.on("error", reject);
      });

      // Step 2: Detect MIME type of the downloaded file
      const buffer = fs.readFileSync(tempFilePath);
      const detectedType = await fileType.fromBuffer(buffer);
      const mimeType = detectedType?.mime || "image/png";
      
      // Step 3: Create Asset node AFTER knowing fileName
      const payload = {
        request: {
          content: {
            name: fileName, // ✅ Now using actual filename
            code: uuidv4(),
            mimeType: mimeType, // ✅ Matches the real file type
            mediaType: "image",
            contentType: "Asset",
            framework: this.framework,
            createdBy: userId || "",
          },
        },
      };

      const headers = {
        "Content-Type": "application/json",
        tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
        Authorization: `Bearer ${userToken}`,
        "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
      };

      const createAssetResponse = await axios.post(
        `${this.middlewareUrl}/action/content/v3/create`,
        payload,
        { headers }
      );

      doId = createAssetResponse.data.result.identifier;
      // console.log("Asset created:", doId);

      // Step 4: Prepare multipart form-data for file upload
      const form = new FormData();
      form.append("file", fs.createReadStream(tempFilePath), {
        filename: fileName,
        contentType: mimeType,
      });

      const uploadUrl = `${this.middlewareUrl}/action/content/v3/upload/${doId}`;

      // Step 5: Upload the file
      const uploadResponse = await axios.post(uploadUrl, form, {
        headers: {
          ...form.getHeaders(),
          Authorization: `Bearer ${userToken}`,
          tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
          "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
        },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
      });

      console.log(`✅ Icon uploaded successfully for content ID: ${doId}`);
      return uploadResponse.data;
    } catch (error: any) {
      console.error(`❌ Failed to upload icon for content ID: ${doId}`);
      console.error(error.response?.data || error.message);
      return false;
    } finally {
      // Clean up temp file
      if (tempFilePath && fs.existsSync(tempFilePath)) {
        fs.unlinkSync(tempFilePath);
        // console.log(`🗑️ Temp file deleted: ${tempFilePath}`);
      }
    }
  }
}
