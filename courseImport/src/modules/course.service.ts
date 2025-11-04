import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Course } from "./course.entity";
import { ConfigService } from "@nestjs/config";
import { HttpService } from "@nestjs/axios";
import * as fs from "fs";
import * as path from "path";
import { v4 as uuidv4 } from "uuid"; // ✅ Import UUID for unique code generation
import { Console } from "console";
import axios from "axios";
import { request } from "http";
import { randomBytes } from "crypto";

interface SetType {
  name: string;
  do_id?: string | null;
  identifier?: string; // <-- Add this
  isNew?: boolean; // <-- Add this
  children?: SetType[];
  content?: string[];
  description: string;
  thumb?: string;
  appIcon?: string;
}

interface CourseMetadataType {
  author?: string;
  copyright?: string;
  copyright_year?: number;
  program?: string[];
  domain?: string[];
  subjects?: string[];
  sub_domain?: string;
  course_keywords?: string[];
  content_language?: string[];
  primary_user?: string[];
  target_age_group?: string[];
  course_description?: string;
  course_thumb?: string;
}

@Injectable()
export class CourseService {
  private extractContentRecursively(setItems: SetType[]): string[] {
    const queue: SetType[] = [...setItems];
    const contentList: string[] = [];

    while (queue.length > 0) {
      const current = queue.shift();

      if (!current) continue;

      if (current.content && Array.isArray(current.content)) {
        contentList.push(...current.content);
      }

      if (current.children && Array.isArray(current.children)) {
        queue.push(...current.children);
      }
    }

    return contentList;
  }

  // private extractContentRecursively(setItems: SetType[]): string[] {
  //   let contentList: string[] = [];

  //   for (const item of setItems) {
  //     if (item.content) {
  //       contentList.push(...item.content);
  //     }

  //     if (item.children && item.children.length > 0) {
  //       contentList.push(...this.extractContentRecursively(item.children));
  //     }
  //   }

  //   return contentList;
  // }
  private readonly logger = new Logger(CourseService.name);
  private readonly logFile = path.join(__dirname, "../../logs/migration.log");
  private courseSetsTable: string;
  private readonly middlewareUrl: string;
  private readonly framework: string;

  constructor(
    @InjectRepository(Course) private courseRepository: Repository<Course>,
    private readonly httpService: HttpService,
    private readonly configService: ConfigService
  ) {
    this.courseSetsTable =
      this.configService.get<string>("COURSE_SETS_TABLE") || "tamil_course";
    this.logger.log(`Using course sets table: ${this.courseSetsTable}`); // ✅ Add this here
    this.middlewareUrl = this.configService.get<string>("MIDDLEWARE_QA") || "";
    this.framework =
      this.configService.get<string>("FRAMEWORK") || "scp-framework";
  }

  async importCourses(limit = 1) {
    this.logger.log(`🚀 Starting course import with limit: ${limit}`);

    // ✅ List of all language-specific tables
    const courseTables = [
      "courses_import_courses"
    ];

    const frameworkData = await this.fetchFrameworkDetails();

    for (const tableName of courseTables) {
      this.logger.log(`\n📂 Processing table: ${tableName}`);
      this.courseSetsTable = tableName;

      try {
        // ✅ Ensure table exists (skip if not)
        const tableCheck = await this.courseRepository.query(`
        SHOW TABLES LIKE '${tableName}'
      `);
        if (tableCheck.length === 0) {
          this.logger.warn(`⚠️ Table ${tableName} does not exist. Skipping...`);
          continue;
        }

        // ✅ Fetch courses from current table
        const courses = await this.courseRepository.query(`
        SELECT id, course_title, course_do_id, status, course_metadata, sets_do_id
        FROM ${tableName}
        WHERE status IN ('pending', 'in_progress', 'failed')
        LIMIT ${limit}
      `);

        if (courses.length === 0) {
          this.logger.log(`No new courses to import from ${tableName}.`);
          continue;
        }

        for (const course of courses) {
          try {
            this.logger.log(`📌 Processing course: ${course.course_title}`);

            if (!course.course_do_id) {
              // ✅ Mark as in_progress
              await this.courseRepository.query(
                `UPDATE ${tableName} SET status = 'in_progress' WHERE id = ?`,
                [course.id]
              );
              let course_do_id = "";

              // Here we need to check already existing courses by name to avoid duplicates with sets we need to cheeck using the API
              // 1) Try to find an existing course in middleware by title
              const existingCoursePresent = await this.findCourseByTitle(
                course
              );

              if (existingCoursePresent.count && 0) {
                let setsParsed = course.sets_do_id;

                if (typeof setsParsed === "string") {
                  try {
                    setsParsed = JSON.parse(setsParsed);
                  } catch (e) {
                    this.logger.error(
                      `❗ Invalid JSON in sets_do_id for course: ${course.course_title}`
                    );
                    throw e;
                  }
                }

                course_do_id = existingCoursePresent.identifier;

                const updateCourse = await this.updateCourse(
                  course_do_id,
                  setsParsed,
                  frameworkData,
                  course
                );

                if (updateCourse) {
                  this.logger.log(
                    `✅ Course updated: ${course.course_title} (ID: ${course_do_id})`
                  );
                } else {
                  this.logger.warn(
                    `⚠️ Course update failed: ${course.course_title} (ID: ${course_do_id})`
                  );

                  throw new Error("Course update failed");
                }
              } else {
                // ✅ Create new course
                course_do_id = await this.createCourse(course, frameworkData);
              }

              // ✅ Save course_do_id + mark completed
              await this.courseRepository.query(
                `UPDATE ${tableName} SET course_do_id = ?, status = 'completed' WHERE id = ?`,
                [course_do_id, course.id]
              );

              this.logger.log(
                `✅ Course created: ${course.course_title} (ID: ${course_do_id})`
              );

              // ✅ Review + Publish
              const userToken = this.configService.get("ACCESS_TOKEN");
              if (userToken) {
                try {
                  await this.retryRequest(
                    () => this.reviewContent(course_do_id, userToken),
                    3,
                    2000,
                    "Review Content"
                  );
                  await this.retryRequest(
                    () => this.publishContent(course_do_id, userToken),
                    3,
                    2000,
                    "Publish Content"
                  );
                  this.logger.log(
                    `🚀 Review + Publish done for course: ${course_do_id}`
                  );
                } catch (error: unknown) {
                  // this.logger.error(
                  //   `❌ Review/Publish failed for ${course_do_id}: ${
                  //     error instanceof Error ? error.message : "Unknown error"
                  //   }`
                  // );
                }
              }
            } else {
              this.logger.log(
                `ℹ️ Course already exists: ${course.course_title} (ID: ${course.course_do_id})`
              );
            }
          } catch (error: any) {
            this.logError(
              `Failed to create course ${
                course.course_title
              } from ${tableName}: ${
                error instanceof Error ? error.message : "Unknown error"
              }`
            );
            await this.courseRepository.query(
              `UPDATE ${tableName} SET status = 'failed' WHERE id = ?`,
              [course.id]
            );
          }
        }
      } catch (err) {
        this.logger.error(
          `❌ Error processing table ${tableName}: ${
            err instanceof Error ? err.message : err
          }`
        );
      }
    }

    this.logger.log("🏁 Course import completed for all tables.");
  }

  /**
   * The main function to handle updating an existing course.
   * It fetches the existing hierarchy and merges the new set data.
   * @param courseId The identifier of the course to update.
   * @param newCourseData The new course data containing sets.
   * @param frameworkData Framework metadata.
   * @returns The result of the update hierarchy API call.
   */
  // ...existing code...
  async updateCourse(
    courseId: string,
    newCourseData: { sets: SetType[] } | null,
    frameworkData: any,
    course: any
  ): Promise<any> {
    try {
      if (!newCourseData || !newCourseData.sets) {
        this.logger.log(
          `No new sets data provided for course ${courseId}. Skipping update.`
        );
        return null;
      }

      // --- Fetch current hierarchy ---
      const existingCourseHierarchy = await this.getHierarchyUsingDoId(
        courseId
      );
      const courseNode =
        existingCourseHierarchy.content || existingCourseHierarchy;

      // --- Update course metadata fields ---
      if (course && course.course_metadata) {
        let courseMetadata: CourseMetadataType = {};
        if (typeof course.course_metadata === "string") {
          try {
            courseMetadata = JSON.parse(course.course_metadata);
          } catch (e) {
            this.logger.error(
              `❗ Invalid JSON in course_metadata for course: ${course.course_title}`
            );
          }
        } else {
          courseMetadata = course.course_metadata;
        }

        if (courseMetadata.program) courseNode.program = courseMetadata.program;
        if (courseMetadata.course_keywords)
          courseNode.keywords = courseMetadata.course_keywords;
        if (courseMetadata.primary_user)
          courseNode.primaryUser = courseMetadata.primary_user;
        if (courseMetadata.target_age_group)
          courseNode.targetAgeGroup = courseMetadata.target_age_group;
        if (courseMetadata.content_language)
          courseNode.contentLanguage = courseMetadata.content_language;
        if (courseMetadata.course_description)
          courseNode.description = courseMetadata.course_description;

        // --- Upload course appIcon if course_thumb present ---
        if (courseMetadata.course_thumb) {
          const iconUploaded = await this.uploadIcon(
            courseMetadata.course_thumb,
            this.configService.get("ACCESS_TOKEN"),
            this.configService.get("CREATED_BY") || ""
          );
          courseNode.appIcon = iconUploaded || "";
        }
      }

      // --- Ensure children exists ---
      if (!courseNode.children) courseNode.children = [];

      // --- Recursive creator for new nodes (with AWS upload) ---
      const createNodeRecursively = async (node: SetType): Promise<SetType> => {
        const createdNode: SetType = { ...node };
        if (!createdNode.identifier) {
          createdNode.identifier =
            createdNode.do_id ||
            `set_${Math.random().toString(36).substr(2, 9)}`;
          createdNode.isNew = true;
        }

        // ✅ Upload icon if thumb present
        if (createdNode.thumb) {
          const uploadedUrl = await this.uploadIcon(
            createdNode.thumb,
            this.configService.get("ACCESS_TOKEN"),
            this.configService.get("CREATED_BY") || ""
          );
          createdNode.appIcon = uploadedUrl || createdNode.thumb;
        }

        // Recurse into children
        if (createdNode.children && createdNode.children.length) {
          createdNode.children = await Promise.all(
            createdNode.children.map((child) => createNodeRecursively(child))
          );
        }

        return createdNode;
      };

      // --- Merge new sets into hierarchy ---
      const mergeHierarchy = async (
        existingNodes: SetType[],
        newNodes: SetType[]
      ): Promise<void> => {
        for (const newNode of newNodes) {
          let matchNode = existingNodes.find(
            (node: SetType) => node.name === newNode.name
          );

          if (matchNode) {
            if (!matchNode.children) matchNode.children = [];
            if (newNode.children && newNode.children.length) {
              await mergeHierarchy(matchNode.children, newNode.children);
            }
            // merge content
            if (newNode.content && newNode.content.length) {
              matchNode.content = [
                ...(matchNode.content || []),
                ...newNode.content,
              ];
            }
          } else {
            existingNodes.push(await createNodeRecursively(newNode));
          }
        }
      };

      await mergeHierarchy(courseNode.children, newCourseData.sets);

      // --- Recursively build payload ---
      function buildPayload(
        node: any,
        isRoot = false,
        nodesModified: Record<string, any> = {},
        hierarchy: Record<string, any> = {}
      ) {
        if (!node) return;
        const nodeId = node.identifier || node.do_id;
        if (!nodeId) return;

        // Only add root and new nodes to nodesModified
        if (isRoot || !!node.isNew) {
          nodesModified[nodeId] = {
            root: isRoot,
            objectType: node.objectType || (isRoot ? "Content" : "Collection"),
            metadata: {
              mimeType:
                node.mimeType || "application/vnd.ekstep.content-collection",
              code: node.code || nodeId,
              name: node.name,
              visibility: isRoot ? "Default" : "Parent",
              contentType: isRoot ? "Course" : "CourseUnit",
              primaryCategory: isRoot ? "Course" : "Course Unit",
              appIcon: node.appIcon || node.thumb || "",
              attributions: [],
              description: node.description || "",
              ...(isRoot ? courseNode.metadata : {}),
            },
            isNew: !!node.isNew,
          };
        }

        // Always add to hierarchy (children + content IDs)
        hierarchy[nodeId] = {
          name: node.name,
          children: [
            ...(node.children || []).map((c: any) => c.identifier || c.do_id),
            ...(node.content || []),
          ],
          root: isRoot,
        };

        // Recurse children
        if (node.children && node.children.length) {
          for (const child of node.children) {
            buildPayload(child, false, nodesModified, hierarchy);
          }
        }
      }

      const nodesModified: Record<string, any> = {};
      const hierarchy: Record<string, any> = {};
      buildPayload(courseNode, true, nodesModified, hierarchy);

      console.log("nodesModified:", JSON.stringify(nodesModified, null, 2));
      console.log("hierarchy:", JSON.stringify(hierarchy, null, 2));

      // --- Prepare payload ---
      const payload = {
        request: {
          data: {
            nodesModified,
            hierarchy,
            lastUpdatedBy: this.configService.get("CREATED_BY"),
          },
        },
      };

      // --- Call Sunbird update API ---
      const result = await this.updateHierarchyUsingDoId(courseId, payload);
      this.logger.log(`✅ Successfully updated course: ${courseId}`);
      return result;
    } catch (error: any) {
      this.logger.error(
        `❌ Failed to update course ${courseId}:`,
        error.message
      );
      throw error;
    }
  }

  // --- New functions for course update logic ---

  /**
   * Fetches the complete hierarchy of a course from the Sunbird API.
   * @param courseId The identifier of the course.
   * @returns The course hierarchy object.
   */
  private async getCourseHierarchy(courseId: string): Promise<any> {
    const apiUrl = `${this.configService.get(
      "FRONTEND_URL"
    )}/action/content/v3/hierarchy/${courseId}?mode=edit`;
    const authToken = this.configService.get("ACCESS_TOKEN");

    try {
      const response = await axios.get(apiUrl, {
        headers: {
          Authorization: `Bearer ${authToken}`,
          tenantId: this.configService.get("MIDDLEWARE_TENANT_ID"),
          "X-Channel-Id": this.configService.get("CREATED_FOR") || "",
          "Content-Type": "application/json",
        },
      });
      return response.data.result.content;
    } catch (error: any) {
      this.logger.error(
        `Failed to fetch hierarchy for ${courseId}:`,
        error.response?.data || error.message
      );
      throw new Error("Failed to fetch course hierarchy");
    }
  }

  /**
   * Recursively updates an existing set by merging new content and children.
   * This function matches sets by name.
   * @param existingSet The set from the current course hierarchy.
   * @param newSet The set from the new data to be merged.
   */
  private updateExistingSet(existingSet: SetType, newSet: SetType): void {
    const existingContentSet = new Set(existingSet.content || []);
    for (const contentId of newSet.content || []) {
      existingContentSet.add(contentId);
    }
    existingSet.content = Array.from(existingContentSet);

    if (newSet.children && newSet.children.length > 0) {
      if (!existingSet.children) {
        existingSet.children = [];
      }
      for (const newChild of newSet.children) {
        const existingChild = existingSet.children.find(
          (child) => child.name === newChild.name
        );

        if (existingChild) {
          this.updateExistingSet(existingChild, newChild);
        } else {
          const newCreatedChild = this.createSetFromData(newChild);
          existingSet.children.push(newCreatedChild);
        }
      }
    }
  }

  /**
   * A helper function to create a new set object.
   * This logic should be similar to your `createSets` logic for a single set.
   * @param setData The new set data.
   * @returns A new SetType object.
   */
  private createSetFromData(setData: SetType): SetType {
    this.logger.log(`Creating new set: ${setData.name}`);
    return setData;
  }

  async CourseIssueFixes(limit = 51) {
    const tables = [
      "asam_course",
      "bengali_course",
      "english_course",
      "gujrati_course",
      "hindi_course",
      "kannada_course",
      "marathi_course",
      "odiya_course",
      "panjabi_course",
      "tamil_course",
      "telugu_course",
      "urdu_course",
    ];

    this.logger.log(`Starting CourseIssueFixes for ${tables.length} tables`);

    for (const tableName of tables) {
      this.logger.log(`📁 Processing table: ${tableName}`);
      await this.CourseIssueFixesOneByone(limit, tableName);
    }

    this.logger.log(`✅ CourseIssueFixes completed for all tables`);
  }

  async CourseIssueFixesOneByone(limit = 51, tableName = this.courseSetsTable) {
    this.logger.log(`🚀 Starting CourseIssueFixes with limit: ${limit}`);

    const courses = await this.courseRepository.query(`
    SELECT id, course_title, course_do_id, status 
    FROM ${tableName}
    WHERE status IN ('completed') AND fix_issue = 0
    LIMIT ${limit}
  `);

    if (courses.length === 0) {
      this.logger.log("✅ No new courses to fix.");
      return;
    }

    for (const course of courses) {
      const { course_do_id, course_title } = course;
      this.logger.log(`🛠 Fixing course: ${course_title} (${course_do_id})`);

      try {
        const response = await this.getHierarchyUsingDoId(course_do_id);

        const { content } = response;

        if (!content) {
          this.logger.warn(`⚠️ Skipping ${course_do_id} — content not found`);
          continue;
        }

        // Normalize "program" to array
        const rawProgram = content.program;
        const newProgram =
          typeof rawProgram === "string"
            ? rawProgram.split(",").map((p) => p.trim())
            : Array.isArray(rawProgram)
            ? rawProgram
            : [];

        const nodesModified = this.buildNodesModified(content, newProgram);
        const hierarchy = this.buildHierarchy(content);

        const payload = {
          request: {
            data: {
              nodesModified,
              hierarchy,
              lastUpdatedBy: "b33d7398-84cb-4072-8cd3-e57c8c000ca2",
            },
          },
        };

        const result = await this.updateHierarchyUsingDoId(
          course_do_id,
          payload
        );

        const userToken = this.configService.get("ACCESS_TOKEN");
        await this.retryRequest(
          () => this.reviewContent(course_do_id, userToken),
          3,
          2000,
          "Review Content"
        );
        await this.retryRequest(
          () => this.publishContent(course_do_id, userToken),
          3,
          2000,
          "Publish Content"
        );
        this.logger.log(
          `✅ Review and publish completed for course: ${course_do_id}`
        );

        await this.updateFixIssueFlag(course_do_id, 1, tableName);

        this.logger.log(
          `✅ Updated program for ${course_title} (${course_do_id})`
        );
      } catch (error: any) {
        this.logger.log(
          `❌ Failed to update ${course_do_id}:`,
          error?.message || error
        );
      }
    }

    this.logger.log("🏁 CourseIssueFixes completed.");
  }

  private buildHierarchy(content: any): Record<string, any> {
    const hierarchy: Record<string, any> = {};

    const process = (node: any, isRoot = false) => {
      if (!node || !node.identifier) return;

      hierarchy[node.identifier] = {
        name: node.name,
        children: (node.children || []).map((child: any) => child.identifier),
        root: isRoot,
      };

      if (node.children && node.children.length) {
        for (const child of node.children) {
          process(child);
        }
      }
    };

    process(content, true);
    console.log(hierarchy);
    return hierarchy;
  }

  private buildNodesModified(
    content: any,
    newProgram: string[],
    updateCreatedBy = false
  ) {
    const rootId = content.identifier;

    // Remove invalid fields for update
    const {
      children, // ❌ remove
      collections, // ❌ remove
      ...cleanContent
    } = content;

    // Start metadata with existing + overwrite program
    const metadata: any = {
      ...cleanContent,
      program: newProgram,
    };

    // Optionally overwrite createdBy
    if (updateCreatedBy) {
      metadata.createdBy =
        this.configService.get("CREATED_BY") || cleanContent.createdBy;
    }

    // Build nodesModified block
    const nodesModified: Record<string, any> = {
      [rootId]: {
        metadata,
        isNew: false,
        objectType: "Content",
        root: true,
      },
    };

    return nodesModified;
  }

  private async retryRequest<T>(
    fn: () => Promise<T>,
    retries = 3,
    delayMs = 2000,
    label = "Request"
  ): Promise<T> {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        return await fn();
      } catch (error) {
        if (attempt === retries) {
          this.logger.error(`❌ ${label} failed after ${retries} attempts.`);
          throw error;
        }
        this.logger.warn(
          `⚠️ ${label} failed on attempt ${attempt}. Retrying in ${delayMs}ms...`
        );
        await new Promise((res) => setTimeout(res, delayMs));
      }
    }
    throw new Error(`${label} failed unexpectedly`);
  }

  private async updateFixIssueFlag(
    course_do_id: string,
    value: number,
    tableName = this.courseSetsTable
  ) {
    await this.courseRepository.query(
      `UPDATE ${tableName} SET fix_issue = ? WHERE course_do_id = ?`,
      [value, course_do_id]
    );
  }

  async getHierarchyUsingDoId(contentId: string): Promise<any> {
    try {
      const userToken = this.configService.get("ACCESS_TOKEN");
      const headers = {
        Authorization: `Bearer ${userToken}`,
        tenantId: this.configService.get("MIDDLEWARE_TENANT_ID"),
        "X-Channel-Id": this.configService.get("CREATED_FOR") || "",
        "Content-Type": "application/json",
      };

      const baseUrl = this.configService.get("MIDDLEWARE_QA");
      const url = `${baseUrl}/action/content/v3/hierarchy/${contentId}`;

      this.logger.log(`🔍 GET Hierarchy: ${url}`);

      const response = await this.httpService.get(url, { headers }).toPromise();
      // this.logger.log(response.data.result);
      return response.data.result || {};
    } catch (error) {
      this.logger.error(
        error instanceof Error ? error.message : JSON.stringify(error)
      );

      throw error;
    }
  }

  async updateHierarchyUsingDoId(
    contentId: string,
    payload: any
  ): Promise<any> {
    try {
      const userToken = this.configService.get("ACCESS_TOKEN");
      const headers = {
        Authorization: `Bearer ${userToken}`,
        tenantId: this.configService.get("MIDDLEWARE_TENANT_ID"),
        "X-Channel-Id": this.configService.get("CREATED_FOR") || "",
        "Content-Type": "application/json",
        "X-Source": "web",
      };

      const baseUrl = this.configService.get("MIDDLEWARE_QA");
      const url = `${baseUrl}/action/content/v3/hierarchy/update`;
      console.log("Update Hierarchy URL:", url);

      this.logger.log(`📤 PATCH Update Hierarchy: ${url}`);
      // this.logger.debug(`Payload: ${JSON.stringify(payload, null, 2)}`);

      const response = await this.httpService
        .patch(url, payload, { headers })
        .toPromise();
      return response.data;
    } catch (error) {
      this.logger.error(
        error instanceof Error ? error.message : JSON.stringify(error)
      );

      throw error;
    }
  }

  private async reviewContent(contentId: string, userToken: string) {
    try {
      // ✅ Prepare headers
      const headers = {
        Authorization: `Bearer ${userToken}`,
        tenantId: this.configService.get("MIDDLEWARE_TENANT_ID"),
        "X-Channel-Id": this.configService.get("CREATED_FOR") || "",
        "Content-Type": "application/json",
      };

      // ✅ Construct API URL
      const middlewareUrl = this.configService.get("MIDDLEWARE_QA");
      const reviewUrl = `${middlewareUrl}/action/content/v3/review/${contentId}`;

      console.log("Calling reviewContent API:", reviewUrl);

      // ✅ Construct request body
      const body = {
        request: {
          content: {}, // ✅ Correct payload format
        },
      };

      console.log("Review API Request Payload:", JSON.stringify(body, null, 2));

      // ✅ Use `PATCH` instead of `POST`
      const response = await this.httpService
        .post(reviewUrl, body, { headers })
        .toPromise();

      console.log(
        "Review API Response:",
        JSON.stringify(response.data, null, 2)
      );

      return response.data;
    } catch (error) {
      /// this.handleApiError('reviewContent', error, contentId);
    }
  }

  private async publishContent(contentId: string, userToken: string) {
    try {
      const headers = {
        Authorization: `Bearer ${userToken}`,
        tenantId: this.configService.get("MIDDLEWARE_TENANT_ID"),
        "X-Channel-Id": this.configService.get("CREATED_FOR") || "",
        "Content-Type": "application/json",
      };

      const middlewareUrl = this.configService.get("MIDDLEWARE_QA");
      const publishUrl = `${middlewareUrl}/action/content/v3/publish/${contentId}`;
      const userId = this.configService.get<string>("CREATED_BY") || "";

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

      console.log(
        "Publish API Request Payload:",
        JSON.stringify(body, null, 2)
      );

      const response = await this.httpService
        .post(publishUrl, body, { headers })
        .toPromise();

      console.log(
        "Publish API Response:",
        JSON.stringify(response.data, null, 2)
      );

      return response.data;
    } catch (error: any) {
      this.logError(
        `Error calling reviewContent API for contentId ${contentId}: ${error.message}`
      );
      if (error.response) {
        console.log(
          "API Error Response:",
          JSON.stringify(error.response.data, null, 2)
        );
      }
    }
  }
  async createCourse(course: any, frameworkData: any): Promise<string> {
    const endpoint = `${this.configService.get(
      "MIDDLEWARE_QA"
    )}/action/content/v3/create`;
    const courseCode = uuidv4(); // ✅ Generate a unique code for the course
    let courseIconUrl = ""; // ✅ Initialize courseIconUrl

    // ✅ Get course_metadata and parse
    let courseMetadata: CourseMetadataType = {};

    if (course.course_metadata) {
      if (typeof course.course_metadata === "string") {
        try {
          courseMetadata = JSON.parse(course.course_metadata);
        } catch (e) {
          this.logger.error(
            `❗ Invalid JSON in course_metadata for course: ${course.course_title}`
          );
          throw e;
        }
      } else {
        courseMetadata = course.course_metadata;
      }
    }

    course.appIcon = "";
    if (courseMetadata.course_thumb) {
      const iconUploaded = await this.uploadIcon(
        courseMetadata.course_thumb,
        this.configService.get("ACCESS_TOKEN"),
        this.configService.get("CREATED_BY") || ""
      );
      courseIconUrl = iconUploaded ? iconUploaded : "";
      course.appIcon = courseIconUrl;
    }

    const payload = {
      request: {
        content: {
          code: courseCode,
          name: course.course_title,
          appIcon: course.appIcon,
          description: "No description available",
          createdBy: this.configService.get("CREATED_BY") || "",
          createdFor: [this.configService.get("CREATED_FOR") || ""],
          mimeType: "application/vnd.ekstep.content-collection",
          resourceType: "Course",
          primaryCategory: "Course",
          contentType: "Course",
          framework: "level1-framework",
          targetFWIds: [this.configService.get("TARGETED_FOR") || ""],
        },
      },
    };

    const headers = {
      Authorization: `Bearer ${this.configService.get("ACCESS_TOKEN")}`,
      tenantId: this.configService.get("MIDDLEWARE_TENANT_ID"),
      "X-Channel-Id": this.configService.get("CREATED_FOR") || "qa-scp-channel",
      "Content-Type": "application/json",
    };

    this.logger.log(`API Endpoint: ${endpoint}`);
    this.logger.log(`Request Headers: ${JSON.stringify(headers, null, 2)}`);
    this.logger.log(`Request Payload: ${JSON.stringify(payload, null, 2)}`);

    let response;
    const maxRetries = 10;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        response = await this.httpService
          .post(endpoint, payload, { headers })
          .toPromise();

        // ✅ Log success response
        this.logger.log(
          `API Response: ${JSON.stringify(response.data, null, 2)}`
        );

        const course_do_id = response.data.result.node_id;

        await this.createSets(
          course_do_id,
          course,
          frameworkData,
          courseMetadata.content_language
        );

        return course_do_id;
      } catch (error: any) {
        const status = error.response?.status;

        if (status === 500 && attempt < maxRetries) {
          this.logger.warn(`Attempt ${attempt} failed with 500. Retrying...`);
          await new Promise((resolve) => setTimeout(resolve, 1000)); // 1 sec delay
        } else {
          if (error.response) {
            this.logError(
              `API Error: ${error.response.status} - ${error.response.statusText}`
            );
            this.logError(
              `API Error Response: ${JSON.stringify(
                error.response.data,
                null,
                2
              )}`
            );
          } else {
            this.logError(`Course creation failed: ${error.message}`);
          }
          throw error;
        }
      }
    }

    // Should never reach here
    throw new Error("Failed to create course after multiple retries.");
  }

  async uploadIcon(
    iconUrl: string,
    userToken: string,
    userId: string
  ): Promise<any> {
    const tempDir = "/tmp";
    let tempFilePath = "";
    let doId = "";

    try {
      const { v4: uuidv4 } = require("uuid");
      const path = require("path");
      const fs = require("fs");
      const fileType = require("file-type");

      // Step 1: Download the image to a temp file
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

      // Step 2: Detect MIME type
      const buffer = fs.readFileSync(tempFilePath);
      const detectedType = await fileType.fromBuffer(buffer);
      const mimeType = detectedType?.mime || "image/png";

      // Step 3: Create Asset node
      const createPayload = {
        request: {
          content: {
            name: fileName,
            code: uuidv4(),
            mimeType: mimeType,
            mediaType: "image",
            contentType: "Asset",
            createdBy: userId || "",
            framework: this.framework,
          },
        },
      };

      const jsonHeaders = {
        "Content-Type": "application/json",
        tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
        Authorization: `Bearer ${userToken}`,
        "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
      };

      const createAssetResponse = await axios.post(
        `${this.middlewareUrl}/action/content/v3/create`,
        createPayload,
        { headers: jsonHeaders }
      );

      doId = createAssetResponse.data.result.identifier;

      const payloadNew = {
        request: {
          content: {
            fileName: fileName, // Must exactly match the file's actual name
          },
        },
      };

      const jsonHeaders1 = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${userToken}`, // ✅ Make sure this is NOT empty
        tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
        "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
      };

      const uploadInitResponse = await axios.post(
        `${this.middlewareUrl}/action/content/v3/upload/url/${doId}`,
        payloadNew,
        { headers: jsonHeaders1 }
      );

      const preSignedUrl = uploadInitResponse.data?.result?.pre_signed_url;
      if (!preSignedUrl) {
        throw new Error("Failed to get pre-signed upload URL");
      }

      // Step 3: Upload file to S3 using PUT
      await axios.put(preSignedUrl, fs.readFileSync(tempFilePath), {
        headers: { "Content-Type": mimeType },
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
      });

      // Step 5: Upload the file to pre-signed URL
      const FormData = require("form-data");

      const form = new FormData();
      form.append(
        "fileUrl",
        preSignedUrl.split("?")[0]
        // `https://prod-knowlg-inquiry-cluster.s3-ap-south-1.amazonaws.com/content/assets//${doId}/${fileName}`
        // `https://knowlg-public.s3-ap-south-1.amazonaws.com/content/assets/${doId}/${fileName}`
      );
      form.append("mimeType", mimeType);

      const uploadResponse = await axios.post(
        `${this.middlewareUrl}/action/asset/v1/upload/${doId}?enctype=multipart/form-data&processData=false&contentType=false&cache=false`,
        form,
        {
          headers: {
            ...form.getHeaders(),
            Authorization: `Bearer ${userToken}`,
            tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
            "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
            "X-Source": "web",
            "X-msgid": uuidv4(),
            Cookie: `authToken=${userToken}; userId=${this.configService.get<string>(
              "CREATED_BY"
            )}; tenantId=${this.configService.get<string>(
              "MIDDLEWARE_TENANT_ID"
            )}`, // <-- Use browser cookies
          },
          maxContentLength: Infinity,
          maxBodyLength: Infinity,
          withCredentials: true,
        }
      );

      console.log(`✅ Icon uploaded successfully for content ID: ${doId}`);
      if (
        uploadResponse.data?.params?.status === "successful" &&
        uploadResponse.data?.result?.content_url
      ) {
        return uploadResponse.data.result.content_url;
      } else {
        throw new Error(
          `Upload failed: ${
            uploadResponse.data?.params?.errmsg || "Unknown error"
          }`
        );
      }
    } catch (error: any) {
      console.error(`❌ Failed to upload icon for content ID: ${doId}`);
      console.error(error.response?.data || error.message);
      return false;
    } finally {
      if (tempFilePath && fs.existsSync(tempFilePath)) {
        fs.unlinkSync(tempFilePath);
      }
    }
  }

  // Add this function to perform the Sunbird content upload step after multipart upload
  // private async registerFileWithSunbird(
  //   doId: string,
  //   fileUrl: string,
  //   mimeType: string,
  //   userToken: string
  // ): Promise<any> {
  //   const uploadUrl = `${this.middlewareUrl}/action/content/v3/upload/${doId}`;
  //   const FormData = require("form-data");
  //   const form = new FormData();
  //   form.append("fileUrl", fileUrl);
  //   form.append("mimeType", mimeType);
  //   const headers = {
  //     ...form.getHeaders(),
  //     Authorization: `Bearer ${userToken}`,
  //     tenantId: this.configService.get<string>("MIDDLEWARE_TENANT_ID"),
  //     "X-Channel-Id": this.configService.get<string>("X_CHANNEL_ID"),
  //   };
  //   try {
  //     const response = await axios.post(uploadUrl, form, { headers });
  //     // console.log('Sunbird content upload response:', response.data);
  //     return response.data;
  //   } catch (err) {
  //     if (
  //       err &&
  //       typeof err === "object" &&
  //       "response" in err &&
  //       err.response &&
  //       typeof err.response === "object" &&
  //       "data" in err.response
  //     ) {
  //       console.error("❌ Error in Sunbird content upload:", err.response.data);
  //     } else {
  //       console.error("❌ Error in Sunbird content upload:", err);
  //     }
  //     const errMsg =
  //       err &&
  //       typeof err === "object" &&
  //       "message" in err &&
  //       typeof err.message === "string"
  //         ? err.message
  //         : String(err);
  //     throw new Error(`Failed to register file with Sunbird: ${errMsg}`);
  //   }
  // }

  async createSets(
    course_do_id: string,
    course: any,
    frameworkData: any,
    contentLanguage: any
  ) {
    console.log(
      "Creating Level 1 sets for course:",
      JSON.stringify(course, null, 2)
    );

    // contentLanguage is already defined as string[]
    // e.g., const contentLanguage = ["Hindi", "English"];

    let languageCondition = "";
    if (contentLanguage && contentLanguage.length > 0) {
      languageCondition = contentLanguage
        .map(
          (lang: any) =>
            `JSON_CONTAINS(JSON_EXTRACT(course_metadata, '$.content_language'), '${JSON.stringify(
              [lang]
            )}')`
        )
        .join(" OR ");
    }

    const query = `
      SELECT sets_do_id, course_metadata
      FROM ${this.courseSetsTable}
      WHERE course_title = ?
        ${languageCondition ? `AND (${languageCondition})` : ""}
    `;

    const courseData = await this.courseRepository.query(query, [
      course.course_title,
    ]);

    if (!courseData.length || !courseData[0].sets_do_id) {
      this.logger.log(`No sets found for course: ${course.course_title}`);
      return;
    }

    const setsDoId = courseData[0].sets_do_id;

    console.log("before parse data");

    let setsParsed;
    if (typeof setsDoId === "string") {
      try {
        setsParsed = JSON.parse(setsDoId);
      } catch (e) {
        this.logger.error(
          `❗ Invalid JSON in sets_do_id for course: ${course.course_title}`
        );
        throw e;
      }
    } else {
      setsParsed = setsDoId;
    }

    const sets: SetType[] = setsParsed.sets || [];

    console.log(sets);

    // ✅ Get course_metadata and parse
    let courseMetadata: CourseMetadataType = {};

    if (courseData[0].course_metadata) {
      if (typeof courseData[0].course_metadata === "string") {
        try {
          courseMetadata = JSON.parse(courseData[0].course_metadata);
        } catch (e) {
          this.logger.error(
            `❗ Invalid JSON in course_metadata for course: ${course.course_title}`
          );
          throw e;
        }
      } else {
        courseMetadata = courseData[0].course_metadata;
      }
    }
    const resolvedIds = this.resolveIdentifiers(courseMetadata, frameworkData);
    /*
    console.log('resolved ids');
    console.log(frameworkData);
    console.log(courseMetadata);
    console.log(resolvedIds);
    */

    //   console.log(courseMetadata);

    const createdSets: Record<string, string> = {}; // Store created set DO IDs

    const rawProgram: any = courseMetadata.program;
    const newProgram =
      typeof rawProgram === "string"
        ? rawProgram.split(",").map((p) => p.trim())
        : Array.isArray(rawProgram)
        ? rawProgram
        : [];

    // ✅ Create Level 1 sets
    const levelNodesModified: Record<string, any> = {
      [course_do_id]: {
        root: true,
        objectType: "Content",
        metadata: {
          appIcon: course.appIcon || "",
          name: course.course_title,
          author: courseMetadata.author || "SCP Channel",
          copyright: courseMetadata.copyright || "SCP Channel",
          copyrightYear: courseMetadata.copyright_year || 2025,
          program: newProgram,
          keywords: courseMetadata.course_keywords || [],
          primaryUser: courseMetadata.primary_user || [],
          targetAgeGroup: courseMetadata.target_age_group || [],
          contentLanguage: courseMetadata.content_language || [],
          description: courseMetadata.course_description || "",
          contentType: "Course",
          primaryCategory: "Course",
          attributions: [],
          targetDomainIds: resolvedIds.targetDomainIds || [],
          targetSubDomainIds: resolvedIds.targetSubDomainIds || [],
          targetSubjectIds: resolvedIds.targetStreamIds || [],
        },
        isNew: false,
      },
    };

    const level1Hierarchy: Record<string, any> = {
      [course_do_id]: {
        // ✅ Add course details to hierarchy
        name: course.course_title,
        children: [],
        root: true,
      },
    };

    await Promise.all(
      sets.map(async (set: SetType) => {
        const set_do_id = uuidv4();

        set.appIcon = "";
        if (set.thumb) {
          const iconUploaded = await this.uploadIcon(
            set.thumb,
            this.configService.get("ACCESS_TOKEN"),
            this.configService.get("CREATED_BY") || ""
          );
          set.appIcon = iconUploaded ? iconUploaded : "";
        }

        levelNodesModified[set_do_id] = {
          root: false,
          objectType: "Collection",
          metadata: {
            mimeType: "application/vnd.ekstep.content-collection",
            code: set_do_id,
            name: set.name,
            visibility: "Parent",
            contentType: "CourseUnit",
            primaryCategory: "Course Unit",
            attributions: [],
            description: set.description,
            thumb: set.thumb || "",
            appIcon: set.appIcon || "",
          },
          isNew: true,
        };

        level1Hierarchy[set_do_id] = {
          name: set.name,
          children: [],
          root: false,
        };

        level1Hierarchy[course_do_id].children.push(set_do_id);
        createdSets[set.name] = set_do_id;
      })
    );

    // console.log('debugging');
    // console.log(course_do_id, level1NodesModified, level1Hierarchy);  // ✅ Log for debugging

    // ✅ API call to create Level 1 sets
    const identifiers = await this.createSetsBatchAPI(
      course_do_id,
      levelNodesModified,
      level1Hierarchy
    );

    if (!identifiers?.data?.result?.identifiers) {
      this.logger.log("Failed to create Level 1 sets.");
      return;
    }

    const identifierMap = identifiers.data.result.identifiers as Record<
      string,
      string
    >;

    // ✅ Update sets with actual do_id from response
    sets.forEach((set: SetType) => {
      const tempId = createdSets[set.name];
      if (tempId && identifierMap[tempId]) {
        set.do_id = identifierMap[tempId];
      }
    });

    //console.log('Updated sets with do_id:', JSON.stringify(sets, null, 2));

    // ✅ Save updated sets with do_id to database
    const setsJson = JSON.stringify({ sets }, null, 2); // Format JSON for readability

    // contentLanguage comes from courseMetadata
    const contentLanguageNew: string[] = courseMetadata.content_language || [];

    let languageConditionNew = "";
    if (contentLanguageNew.length > 0) {
      languageConditionNew = contentLanguageNew
        .map(
          (lang) =>
            `JSON_CONTAINS(JSON_EXTRACT(course_metadata, '$.content_language'), '${JSON.stringify(
              [lang]
            )}')`
        )
        .join(" OR ");
    }

    const queryNew = `
      UPDATE ${this.courseSetsTable}
      SET sets_do_id = ?
      WHERE course_title = ?
        ${languageConditionNew ? `AND (${languageConditionNew})` : ""}
    `;

    await this.courseRepository.query(queryNew, [
      setsJson,
      course.course_title,
    ]);

    console.log("DO IDs saved to database successfully.");

    // ✅ Create Level 2 sets
    // ✅ Check if any Level 1 set has children before calling createSubSets
    const hasChildren = sets.some(
      (set) => set.children && set.children.length > 0
    );

    if (hasChildren) {
      console.log(
        "✅ At least one Level 1 set has children, creating Level 2 sets..."
      );
      await this.createSubSets(
        course_do_id,
        sets,
        course,
        resolvedIds,
        contentLanguageNew
      );
    } else {
      console.log("🚫 No Level 1 sets have children. Skipping createSubSets.");
    }

    await this.associateAllContents(course_do_id, sets);

    this.logger.log(
      `Level 1 and Level 2 sets created for course: ${course.course_title}`
    );
  }

  async createSubSets(
    course_do_id: string,
    sets: SetType[],
    course: any,
    resolvedIds: any,
    contentLanguage: any
  ) {
    console.log("🔧 Creating nested sets for:", course.course_title);

    const nodesModified: Record<string, any> = {};
    const hierarchy: Record<string, any> = {};

    // ✅ Recursive hierarchy builder
    const buildSetTree = async (set: SetType): Promise<string> => {
      const do_id = uuidv4(); // assign new ID
      set.do_id = do_id; // attach it to original structure

      set.appIcon = "";
      if (set.thumb) {
        const iconUploaded = await this.uploadIcon(
          set.thumb,
          this.configService.get("ACCESS_TOKEN"),
          this.configService.get("CREATED_BY") || ""
        );
        set.appIcon = iconUploaded ? iconUploaded : "";
      }

      // create node
      nodesModified[do_id] = {
        root: false,
        objectType: "Collection",
        metadata: {
          mimeType: "application/vnd.ekstep.content-collection",
          code: do_id,
          name: set.name,
          visibility: "Parent",
          contentType: "CourseUnit",
          primaryCategory: "Course Unit",
          attributions: [],
          description: set.description || "",
          thumb: set.thumb || "",
          appIcon: set.appIcon || "",
        },
        isNew: true,
      };

      // recursively process children
      const childIds: string[] = [];
      if (set.children && set.children.length > 0) {
        for (const child of set.children) {
          const childId = await buildSetTree(child); // ✅ await here
          childIds.push(childId);
        }
      }

      hierarchy[do_id] = {
        name: set.name,
        children: childIds,
        root: false,
      };

      return do_id;
    };

    // ✅ Step 1: build from top-level sets
    for (const topLevelSet of sets) {
      const topId = await buildSetTree(topLevelSet);
      hierarchy[topId].root = true;
    }

    console.log("🧱 Final Hierarchy:", JSON.stringify(hierarchy, null, 2));

    // ✅ Step 2: call batch create API
    const identifiers = await this.createSubSetsBatchAPI(
      course_do_id,
      nodesModified,
      hierarchy,
      course,
      resolvedIds,
      contentLanguage
    );

    if (!identifiers) {
      this.logger.log("❌ Failed to create sets.");
      return;
    }

    // ✅ Step 3: replace UUIDs with actual do_ids from API response
    const replaceDoIdInSets = (setList: SetType[]) => {
      for (const set of setList) {
        const actualDoId = identifiers[set.do_id];
        if (actualDoId) {
          set.do_id = actualDoId;
        }
        if (set.children && set.children.length > 0) {
          replaceDoIdInSets(set.children);
        }
      }
    };
    replaceDoIdInSets(sets);

    // ✅ Step 4: save updated structure
    const setsJson = JSON.stringify({ sets }, null, 2);
    // contentLanguage is already defined, e.g. ["Hindi", "English"]
    let languageCondition = "";
    if (contentLanguage && contentLanguage.length > 0) {
      languageCondition = contentLanguage
        .map(
          (lang: string) =>
            `JSON_CONTAINS(JSON_EXTRACT(course_metadata, '$.content_language'), '${JSON.stringify(
              [lang]
            )}')`
        )
        .join(" OR ");
    }

    const query = `
      UPDATE ${this.courseSetsTable}
      SET sets_do_id = ?
      WHERE course_title = ?
        ${languageCondition ? `AND (${languageCondition})` : ""}
    `;

    await this.courseRepository.query(query, [setsJson, course.course_title]);

    console.log("✅ Nested sets stored successfully.");
  }

  private calculateSetDepth(set: SetType): number {
    if (!set.children || set.children.length === 0) {
      return 0;
    }

    return (
      1 +
      Math.max(...set.children.map((child) => this.calculateSetDepth(child)))
    );
  }

  async associateAllContents(course_do_id: string, sets: SetType[]) {
    // Phase 1: Associate content for all Level 1 sets
    for (const set of sets) {
      if (set.do_id && set.content && set.content.length > 0) {
        console.log(
          `📌 Associating content to Level 1 set: ${set.name} (do_id: ${set.do_id})`
        );
        await this.associateContent(course_do_id, set.do_id, set.content);
      }
    }

    // Phase 2: Recursively associate content for children of each Level 1 set
    for (const set of sets) {
      if (set.children && set.children.length > 0) {
        for (const child of set.children) {
          await this.recursivelyAssociateContent(course_do_id, child);
        }
      }
    }

    console.log("✅ All contents associated successfully.");
  }

  private async recursivelyAssociateContent(
    course_do_id: string,
    set: SetType
  ) {
    if (set.do_id && set.content && set.content.length > 0) {
      console.log(
        `📌 Associating content to nested set: ${set.name} (do_id: ${set.do_id})`
      );
      await this.associateContent(course_do_id, set.do_id, set.content);
    }

    if (set.children && set.children.length > 0) {
      for (const child of set.children) {
        await this.recursivelyAssociateContent(course_do_id, child);
      }
    }
  }

  async createSubSetsBatchAPI(
    course_do_id: string,
    nodesModified: any,
    hierarchy: any,
    course: any,
    resolvedIds: any,
    contentLanguage: any
  ) {
    console.log("In createSubSetsBatchAPI");

    const endpoint = `${this.configService.get(
      "MIDDLEWARE_QA"
    )}/action/content/v3/hierarchy/update`;

    // contentLanguage is already defined, e.g., ["Hindi", "English"]
    let languageCondition = "";
    if (contentLanguage && contentLanguage.length > 0) {
      languageCondition = contentLanguage
        .map(
          (lang: string) =>
            `JSON_CONTAINS(JSON_EXTRACT(course_metadata, '$.content_language'), '${JSON.stringify(
              [lang]
            )}')`
        )
        .join(" OR ");
    }

    const query = `
      SELECT course_metadata
      FROM ${this.courseSetsTable}
      WHERE course_title = ?
        ${languageCondition ? `AND (${languageCondition})` : ""}
    `;

    const courseMetaResult = await this.courseRepository.query(query, [
      course.course_title,
    ]);

    let courseMetadata: CourseMetadataType = {};
    if (courseMetaResult[0].course_metadata) {
      if (typeof courseMetaResult[0].course_metadata === "string") {
        try {
          courseMetadata = JSON.parse(courseMetaResult[0].course_metadata);
        } catch (e) {
          this.logger.error(
            `❗ Invalid JSON in course_metadata for course: ${course.course_title}`
          );
          throw e;
        }
      } else {
        courseMetadata = courseMetaResult[0].course_metadata;
      }
    }

    const rawProgram: any = courseMetadata.program;
    const newProgram =
      typeof rawProgram === "string"
        ? rawProgram.split(",").map((p) => p.trim())
        : Array.isArray(rawProgram)
        ? rawProgram
        : [];

    const courseMetadataObj: Record<
      string,
      {
        root: boolean;
        objectType: string;
        metadata: Record<string, any>;
        isNew: boolean;
      }
    > = {
      [course_do_id]: {
        root: true,
        objectType: "Content",
        metadata: {
          appIcon: course.appIcon || "",
          name: course.course_title,
          author: courseMetadata.author || "SCP Channel",
          copyright: courseMetadata.copyright || "SCP Channel",
          copyrightYear: courseMetadata.copyright_year || 2025,
          program: newProgram,
          keywords: courseMetadata.course_keywords || [],
          contentLanguage: courseMetadata.content_language || [],
          description: courseMetadata.course_description || "",
          contentType: "Course",
          primaryCategory: "Course",
          attributions: [],
          targetDomainIds: resolvedIds.targetDomainIds || [],
          targetSubDomainIds: resolvedIds.targetSubDomainIds || [],
          targetSubjectIds: resolvedIds.targetStreamIds || [],
        },
        isNew: false,
      },
    };

    const filteredCourseHierarchy: Record<
      string,
      { name: string; children: string[]; root: boolean }
    > = {
      ...hierarchy,
      [course_do_id]: {
        name: course.course_title,
        children: Object.keys(hierarchy).filter(
          (childId) => !nodesModified[childId] || hierarchy[childId].root
        ),
        root: true,
      },
    };

    const payload = {
      request: {
        data: {
          nodesModified: {
            ...nodesModified,
            ...courseMetadataObj,
          },
          hierarchy: filteredCourseHierarchy,
          lastUpdatedBy: this.configService.get("CREATED_BY") || "",
        },
      },
    };

    const headers = {
      Authorization: `Bearer ${this.configService.get("ACCESS_TOKEN")}`,
      tenantId:
        this.configService.get("MIDDLEWARE_TENANT_ID") ||
        "ef99949b-7f3a-4a5f-806a-e67e683e38f3",
      "X-Channel-Id":
        this.configService.get("CREATED_FOR") || "test-k12-channel",
      "Content-Type": "application/json",
    };

    console.log(
      "Payload for createSubSetsBatchAPI:",
      JSON.stringify(payload, null, 2)
    );

    let attempt = 0;
    const maxRetries = 10;
    while (attempt < maxRetries) {
      try {
        const response = await this.httpService
          .patch(endpoint, payload, { headers })
          .toPromise();
        this.logger.log(
          `API Response: ${JSON.stringify(response.data, null, 2)}`
        );
        return response.data.result.identifiers;
      } catch (error: any) {
        attempt++;
        const status = error?.response?.status;

        if (status === 500 && attempt < maxRetries) {
          this.logger.warn(
            `⚠️ Attempt ${attempt} failed with 500. Retrying...`
          );
          await new Promise((res) => setTimeout(res, 1000)); // wait 1 second before retry
          continue;
        }

        if (error.response) {
          this.logError(
            `API Error: ${error.response.status} - ${error.response.statusText}`
          );
          this.logError(
            `API Error Response: ${JSON.stringify(
              error.response.data,
              null,
              2
            )}`
          );
        } else {
          this.logError(`Subset creation failed: ${error.message}`);
        }

        throw error;
      }
    }
  }

  async createSetsBatchAPI(
    course_do_id: string,
    nodesModified: any,
    hierarchy: any
  ) {
    console.log("In createSetsBatchAPI");

    const endpoint = `${this.configService.get(
      "MIDDLEWARE_QA"
    )}/action/content/v3/hierarchy/update`;

    console.log("Endpoint for createSetsBatchAPI:", endpoint);

    const payload = {
      request: {
        data: {
          nodesModified,
          hierarchy,
          lastUpdatedBy: this.configService.get("CREATED_BY") || "", // ✅ Dynamic `createdBy` from env,
        },
      },
    };

    const headers = {
      Authorization: `Bearer ${this.configService.get("ACCESS_TOKEN")}`,
      tenantId:
        this.configService.get("MIDDLEWARE_TENANT_ID") ||
        "ef99949b-7f3a-4a5f-806a-e67e683e38f3", // ✅ Dynamic or default
      "X-Channel-Id":
        this.configService.get("CREATED_FOR") || "test-k12-channel", // ✅ Dynamic or default
      "Content-Type": "application/json",
    };

    // ✅ Log request for debugging
    this.logger.log(`API Endpoint: ${endpoint}`);
    this.logger.log(`Request Headers: ${JSON.stringify(headers, null, 2)}`);
    this.logger.log(`Request Payload: ${JSON.stringify(payload, null, 2)}`);

    try {
      // 🔄 Changed from POST to PATCH
      const response = await this.httpService
        .patch(endpoint, payload, { headers })
        .toPromise();
      this.logger.log(
        `API Response: ${JSON.stringify(response.data, null, 2)}`
      );

      // ✅ Return the API response to capture identifiers (do_id)
      return response;
    } catch (error: any) {
      if (error.response) {
        this.logError(
          `API Error: ${error.response.status} - ${error.response.statusText}`
        );
        this.logError(
          `API Error Response: ${JSON.stringify(error.response.data, null, 2)}`
        );
      } else {
        this.logError(`Set creation failed: ${error.message}`);
      }
      throw error;
    }
  }

  private logError(message: string) {
    const logDir = path.join(__dirname, "../../logs");
    const logFile = path.join(logDir, "migration.log");
    const logEntry = `[${new Date().toISOString()}] ${message}\n`;

    // ✅ Ensure /logs directory exists
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir);
    }

    // ✅ Create log file if it doesn't exist and append log entries
    fs.appendFileSync(logFile, logEntry);

    // ✅ Log to console
    console.error(message);
  }

  async createSet(parentId: string, setName: string): Promise<string> {
    try {
      const response = await this.httpService
        .patch(
          `${this.configService.get(
            "MIDDLEWARE_QA"
          )}/action/content/v3/hierarchy/update`,
          {
            request: {
              data: {
                nodesModified: {
                  [parentId]: {
                    root: true,
                    objectType: "Content",
                    isNew: false,
                  },
                  [setName]: {
                    root: false,
                    objectType: "Collection",
                    metadata: {
                      name: setName,
                      mimeType: "application/vnd.ekstep.content-collection",
                      contentType: "CourseUnit",
                      primaryCategory: "Course Unit",
                    },
                    isNew: true,
                  },
                },
                hierarchy: { [parentId]: { children: [setName], root: true } },
              },
            },
          }
        )
        .toPromise();

      return setName;
    } catch (error: unknown) {
      if (error instanceof Error) {
        // ✅ Safe type check
        this.logError(`Set creation failed: ${error.message}`);
        throw error;
      } else {
        const errorMsg = typeof error === "string" ? error : "Unknown error";
        this.logError(`Set creation failed: ${errorMsg}`);
        throw new Error(errorMsg);
      }
    }
  }

  async associateContent(
    courseId: string,
    setId: string,
    contentList: string[]
  ) {
    const maxRetries = 5; // 🔁 Number of retries
    const retryDelay = 5000; // ⏳ Delay between retries (in ms)
    let attempt = 0;

    const url = `${this.configService.get(
      "MIDDLEWARE_QA"
    )}/action/content/v3/hierarchy/add`;

    const headers = {
      Authorization: `Bearer ${this.configService.get("ACCESS_TOKEN")}`, // ✅ Corrected Authorization
      "X-Channel-Id":
        this.configService.get("CREATED_FOR") || "test-k12-channel",
      tenantId:
        this.configService.get("MIDDLEWARE_TENANT_ID") ||
        "ef99949b-7f3a-4a5f-806a-e67e683e38f3",
      "Content-Type": "application/json",
    };

    const payload = {
      request: {
        rootId: courseId,
        unitId: setId,
        children: contentList,
      },
    };

    while (attempt < maxRetries) {
      try {
        console.log("📝 Request Headers:", JSON.stringify(headers, null, 2));
        console.log("📦 Request Payload:", JSON.stringify(payload, null, 2));
        console.log(`🔁 Attempt ${attempt + 1} of ${maxRetries}`);

        const response = await this.httpService
          .patch(url, payload, { headers })
          .toPromise();

        console.log(
          "✅ Association Successful. API Response:",
          JSON.stringify(response.data, null, 2)
        );

        return response.data; // ✅ Return on success
      } catch (error: any) {
        console.error(`❌ Attempt ${attempt + 1} failed:`);

        if (error.response) {
          console.error("🔻 API Error Status:", error.response.status);
          console.error(
            "🔻 API Error Data:",
            JSON.stringify(error.response.data, null, 2)
          );
        } else {
          console.error("🔻 Unknown Error:", error.message);
        }

        attempt++;

        if (attempt >= maxRetries) {
          console.error("❌ All retry attempts failed.");
          this.logError(
            `❌ Content association failed after ${maxRetries} attempts: ${error.message}`
          );
          throw error; // ❌ Throw after retries are exhausted
        }

        console.log(`⏳ Retrying in ${retryDelay / 1000} seconds...`);
        await this.delay(retryDelay); // Wait before retrying
      }
    }
  }

  private delay(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async fetchFrameworkDetails() {
    const framework = this.configService.get("TARGETED_FOR");
    const url = `${this.configService.get(
      "MIDDLEWARE_QA"
    )}/api/framework/v1/read/${framework}`;

    try {
      const response = await this.httpService.get(url).toPromise();
      const categories = response.data.result.framework.categories;

      const domainMap =
        categories.find((c: any) => c.code === "domain")?.terms || [];
      const subDomainMap =
        categories.find((c: any) => c.code === "subDomain")?.terms || [];

      const subjectMap: any[] =
        categories.find((c: any) => c.code === "subject")?.terms || [];

      // 👇 Add subjects from domain → associations and subDomain → associations
      const nestedSubjects = [...domainMap, ...subDomainMap]
        .flatMap((term: any) => term.associations || [])
        .filter((assoc: any) => assoc.category === "subject");

      // 👇 Merge top-level and nested subjects
      const allSubjects = [...subjectMap, ...nestedSubjects];

      // 👇 Deduplicate by identifier
      const uniqueSubjectMap = Object.values(
        allSubjects.reduce((acc, curr) => {
          acc[curr.identifier] = curr;
          return acc;
        }, {} as Record<string, any>)
      );

      //  return { domainMap, subDomainMap, subjectMap };
      return { domainMap, subDomainMap, subjectMap: uniqueSubjectMap };
    } catch (error: any) {
      this.logger.error(`Failed to fetch framework details: ${error.message}`);
      throw error;
    }
  }

  resolveIdentifiers(metadata: any, frameworkData: any) {
    const targetDomainIds = frameworkData.domainMap
      .filter((d: any) => d.name === metadata.domain)
      .map((d: any) => d.identifier);

    const targetSubDomainIds = frameworkData.subDomainMap
      .filter((s: any) => metadata.sub_domain.includes(s.name))
      .map((s: any) => s.identifier);

    const targetStreamIds = frameworkData.subjectMap
      .filter((s: any) => metadata.subjects.includes(s.name))
      .map((s: any) => s.identifier);

    console.log("Resolved Subject Identifiers:", targetStreamIds);

    return { targetDomainIds, targetSubDomainIds, targetStreamIds };
  }

  async deleteAllCourses(limit = 1000) {
    try {
      // ✅ Array of table names
      const tables = [
        "courses_import_courses"
      ];

      for (const tableName of tables) {
        console.log(`Processing table: ${tableName}`);

        // ✅ Step 1: Fetch rows
        const rows = await this.courseRepository.query(`
        SELECT id, course_do_id
        FROM ${tableName}
        WHERE course_do_id IS NOT NULL
        ORDER BY id
        LIMIT ${limit}
      `);

        if (!rows.length) {
          console.log(`⚠️ No courses found in ${tableName}`);
          continue;
        }

        console.log(`Found ${rows.length} courses in ${tableName}`);

        // ✅ Step 2: Process each row
        for (const row of rows) {
          const doId = row.course_do_id;

          let success = false;
          let attempts = 0;

          while (!success && attempts < 3) {
            attempts++;
            try {
              const deleteUrl = `${this.configService.get(
                "MIDDLEWARE_QA"
              )}/action/content/v3/retire/${doId}`;

              const deleteRes = await axios.delete(deleteUrl, {
                headers: {
                  Accept: "application/json, text/plain, */*",
                  Authorization: `Bearer ${this.configService.get<string>(
                    "ACCESS_TOKEN"
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
                `✅ Deleted course ${doId} (attempt ${attempts})`,
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
                await this.courseRepository.query(
                  `UPDATE ${tableName}
                    SET course_do_id = NULL, status = 'pending'
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
      this.logError(`Error in deleteAllCourses: ${error.message}`);
      throw error;
    }
  }

  async taxonomyMappingVocationalTrainingCourses(limit = 1) {
    const fetched = await this.getPublishedCourses(limit);

    // Normalize fetched course names and include identifier
    const fetchedCourses =
      fetched?.content?.map((course: any) => ({
        name: course.name?.trim(),
        identifier: course.identifier,
      })) || [];

      const courseIdentifiers =
  fetched?.content?.map((course: any) => course.identifier) || [];

    console.log(`✅ Total courses fetched: ${courseIdentifiers.length}`);
    console.log("Fetched Courses:", courseIdentifiers);

    // // const courseList = [
    // //   "Digital Readiness (English)",
    // //   "Digital Readiness (Hindi)",
    // //   "Digital Readiness (Marathi)",
    // //   "Digital Readiness (Bengali)",
    // //   "Digital Readiness (Odia)",
    // //   "Digital Readiness (Telugu)",
    // //   "Digital Readiness (Assamese)",
    // //   "Digital Readiness (Kannada)",
    // //   "Introduction to Electrician Career (Hindi)",
    // //   "Introduction to Electrician Career (Telugu)",
    // //   "Introduction to Electrician Career (Odia)",
    // //   "Introduction to Electrician Career (Bengali)",
    // //   "Introduction to Electrician Career (Marathi)",
    // //   "Introduction to Electrician Career (Assamese)",
    // //   "Introduction to Electrician Career (Gujarati)",
    // //   "Introduction to Electrician Career (Kannada)",
    // //   "Introduction to Automotive 4 Wheeler Technician Career (Hindi)",
    // //   "Introduction to Automotive 4 Wheeler Technician Career (Telugu)",
    // //   "Introduction to Automotive 4 Wheeler Technician Career (Odia)",
    // //   "Introduction to Automotive 4 Wheeler Technician Career (Bengali)",
    // //   "Introduction to Automotive 4 Wheeler Technician Career (Marathi)",
    // //   "Introduction to Plumbing Industry Career (Hindi)",
    // //   "Introduction to Plumbing Industry Career (Marathi)",
    // //   "Introduction to Plumbing Industry Career (Odia)",
    // //   "Introduction to Plumbing Industry Career (Telugu)",
    // //   "Introduction to Plumbing Industry Career (Bengali)",
    // //   "Introduction to Welding Industry Career (Hindi)",
    // //   "Introduction to Welding Industry Career (Marathi)",
    // //   "Introduction to Welding Industry Career (Bengali)",
    // //   "Introduction to Welding Industry Career (Telugu)",
    // //   "Introduction to Welding Industry Career (Odia)",
    // //   "Introduction to Welding Industry Career (Kannada)",
    // //   "Introduction to Welding Industry Career (Tamil)",
    // //   "Introduction to Healthcare Industry Career (Hindi)",
    // //   "Introduction to Healthcare Industry Career (Telugu)",
    // //   "Introduction to Healthcare Industry Career (Odia)",
    // //   "Introduction to Healthcare Industry Career (Bengali)",
    // //   "Introduction to Healthcare Industry Career (Marathi)",
    // //   "Introduction to Healthcare Industry Career (Kannada)",
    // //   "Introduction to Healthcare Industry Career (Tamil)",
    // //   "Introduction to Healthcare Industry Career (Punjabi)",
    // //   "Introduction to Beautician Career (Hindi)",
    // //   "Introduction to Beautician Career (Marathi)",
    // //   "Introduction to Beautician Career (Bengali)",
    // //   "Introduction to Beautician Career (Odia)",
    // //   "Introduction to Beautician Career (Telugu)",
    // //   "Introduction to Beautician Career (Assamese)",
    // //   "Introduction to Beautician Career (Kannada)",
    // //   "Introduction to Beautician Career (Tamil)",
    // //   "Introduction to False Ceiling & Dry Wall Installer... (Hindi)",
    // //   "Introduction to Food & Beverage Service Career (Hindi)",
    // //   "Introduction to Food & Beverage Service Career (Telugu)",
    // //   "Introduction to Food & Beverage Service Career (Odia)",
    // //   "Introduction to Food & Beverage Service Career (Bengali)",
    // //   "Introduction to Food & Beverage Service Career (Marathi)",
    // //   "Introduction to Food & Beverage Service Career (Kannada)",
    // //   "Introduction to Food & Beverage Service Career (Tamil)",
    // //   "Introduction to Food Production Career (Hindi)",
    // //   "Introduction to Food Production Career (Telugu)",
    // //   "Introduction to Food Production Career (Odia)",
    // //   "Introduction to Food Production Career (Bengali)",
    // //   "Introduction to Food Production Career (Marathi)",
    // //   "Introduction to Food Production Career (Kannada)",
    // //   "Introduction to Food Production Career (Tamil)",
    // //   "Introduction to Housekeeping Career (Hindi)",
    // //   "Introduction to Housekeeping Career (Telugu)",
    // //   "Introduction to Housekeeping Career (Odia)",
    // //   "Introduction to Housekeeping Career (Bengali)",
    // //   "Introduction to Housekeeping Career (Marathi)",
    // //   "Introduction to Housekeeping Career (Kannada)",
    // //   "Introduction to Housekeeping Career (Tamil)",
    // //   "Introduction to 2 Wheeler Technician Career (Hindi)",
    // //   "Introduction to 2 Wheeler Technician Career (Telugu)",
    // //   "Introduction to 2 Wheeler Technician Career (Odia)",
    // //   "Introduction to 2 Wheeler Technician Career (Bengali)",
    // //   "Introduction to 2 Wheeler Technician Career (Marathi)",
    // //   "Introduction to 2 Wheeler Technician Career (Kannada)",
    // //   "Introduction to 2 Wheeler Technician Career (Tamil)",
    // //   "Introduction to Mason Career (Hindi)",
    // //   "Introduction to Bar Bender Career (Hindi)",
    // //   "Introduction to Apparel Industry (Hindi)",
    // //   "Introduction to Apparel Industry (Tamil)",
    // //   "Introduction to Apparel Industry (Kannada)",
    // //   "Introduction to Bar Bender Career (Marathi)",
    // //   "Introduction to Mason Career (Marathi)",
    // //   "Internet Safety: Be Internet Awesome (Marathi)",
    // //   "Internet Safety: Be Internet Awesome (Bengali)",
    // //   "Internet Safety: Be Internet Awesome (Kannada)",
    // //   "Internet Safety: Be Internet Awesome (Telugu)",
    // //   "Internet Safety: Be Internet Awesome (Assamese)",
    // //   "Internet Safety: Be Internet Awesome (Punjabi)",
    // //   "Internet Safety: Be Internet Awesome (Odia)",
    // //   "Internet Safety: Be Internet Awesome (Urdu)",
    // //   "Internet Safety: Be Internet Awesome (Tamil)",
    // //   "English Learning Program For Youth - Level 1 (Marathi)",
    // //   "English Learning Program For Youth - Level 1 (Bengali)",
    // //   "English Learning Program For Youth - Level 1 (Kannada)",
    // //   "English Learning Program For Youth - Level 1 (Telugu)",
    // //   "English Learning Program For Youth - Level 1 (Tamil)",
    // //   "English Learning Program For Youth - Level 1 (Odia)",
    // //   "English Learning Program For Youth - Level 1 (Punjabi)",
    // //   "प्रकल्प व्यवस्थापन | Project Management",
    // //   "गुगल टूल्स वापरून शिका | Learn with Google Tools",
    // //   "नोकरीसाठी तयार राहा (बी जॉब रेडी) | Be Job Ready",
    // //   "इंटरनेट सुरक्षा: इंटरनेट ऑसम व्हा (Be Internet Awesome)",
    // //   "Effective Communication",
    // //   "Project Management",
    // //   "Financial Literacy",
    // //   "Learn with Google Tools",
    // //   "Personality Development",
    // //   "Be Job Ready",
    // //   "Internet Safety: BIA (Be Internet Awesome)",
    // //   "Career Awareness",
    // //   "ಪ್ರಾಜೆಕ್ಟ್ ಮ್ಯಾನೇಜ್ಮೆಂಟ್ | Project Management",
    // //   "ಲರ್ನ್ ವಿಥ್ ಗೂಗಲ್ ಟೂಲ್ಸ್ | Learn with Google Tools",
    // //   "ಬಿ ಜಾಬ್ ರೆಡಿ - ಕೆಲಸಕ್ಕೆ ಸಿದ್ಧರಾಗಿರಿ | Be Job Ready",
    // //   "ಇಂಟರ್ನೆಟ್ ಸುರಕ್ಷತೆ: ಬಿ ಇಂಟರ್ನೆಟ್  ಆಸಂ (Be Internet Awesome) | Internet Safety: Be Internet Awesome",
    // //   "ప్రాజెక్ట్ మేనేజ్మెంట్ | Project Management",
    // //   '"గూగుల్ టూల్స్‌"  ద్వారా  నేర్చుకోవడం ! | Learn with Google Tools',
    // //   "బి జాబ్ రెడీ - ఉద్యోగానికి సిద్ధం కావడం  | Be Job Ready",
    // //   "ఇంటర్నెట్ భద్రత: ఇంటర్నెట్ ఆసమ్ గా మారండి | Internet Safety : Be Internet Awesome",
    // //   "திட்ட மேலாண்மை | Project Management",
    // //   "கூகுள் டூல்ஸ் மூலம் கற்றல் | Learn with Google Tools",
    // //   "இணைய பாதுகாப்பு : BIA (Be Internet Awesome)",
    // //   "வேலைக்கு தயாராக இருங்கள் |  Be Job Ready",
    // //   "પ્રોજેક્ટ મેનેજમેન્ટ | Project Management",
    // //   "લર્ન વિથ ગૂગલ ટૂલ્સ  | Learn with Google Tools",
    // //   "બી જૉબ રેડી - નોકરી માટે તૈયાર રહો |  Be Job Ready",
    // //   "ઈન્ટરનેટ સેફટી: ઈન્ટરનેટ ઑસમ બનો.(Be Internet Awesome)",
    // //   "ਪ੍ਰੋਜੈਕਟ ਪ੍ਰਬੰਧਨ | Project Management",
    // //   "ਲਰਨ ਵਿਦ ਗੂਗਲ ਟੂਲਸ | Learn with Google Tools",
    // //   "ਬੀ ਜੌਬ ਰੇਡੀ - ਨੌਕਰੀ ਦੇ ਲਈ ਤਿਆਰ ਹੋਣਾ |  Be Job Ready",
    // //   "ਇੰਟਰਨੈੱਟ ਸੇਫ਼ਟੀ: ਬਣੋ ਇੰਟਰਨੈੱਟ ਆੱਸਮ | Internet Safety",
    // //   "ପ୍ରୋଜେକ୍ଟ ମ୍ୟାନେଜମେଣ୍ଟ | Project Management",
    // //   "ଲର୍ଣ୍ଣ ଉଇଥ୍ ଗୁଗଲ୍ ଟୁଲ୍ସ  | Learn with Google Tools",
    // //   "ବି ଜବ୍ ରେଡ଼ି - ଚାକିରି ପାଇଁ ପ୍ରସ୍ତୁତ ରୁହ | Be Job Ready",
    // //   "ଇଣ୍ଟର୍ନେଟ୍ ସୁରକ୍ଷା: 'ଇଣ୍ଟରନେଟ୍'ରେ ପାରଦର୍ଶୀ ହୁଅନ୍ତୁ (Be Internet Awesome)  | Internet Safety",
    // //   "প্রজেক্ট ম্যানেজমেন্ট | Project Management",
    // //   "লার্ন উইথ গুগল টুলস | Learn with Google Tools",
    // //   "বি জব রেডি |  Be Job Ready",
    // //   "ইন্টারনেট নিরাপত্তা: হয়ে উঠুন ইন্টারনেট পারদর্শী | Internet Safety",
    // //   "پراجیکٹ مینجمنٹ | Project Management",
    // //   "لرن وتھ گوگل ٹولس | Learn with Google Tools",
    // //   "بی جاب ریڈی - نوکری کے لیے تیار رہیں- | Be Job Ready",
    // //   "انٹرنیٹ سیفٹی: بنیں انٹرنیٹ اوسم | Internet Safety",
    // // ].map((name) => name.trim());

    // // Build a Set for faster lookup (optional but recommended)
    // const courseSet = new Set(courseList.map((name) => name.toLowerCase()));

    // // Filter courses NOT in courseList
    // const notInCourseList = fetchedCourses.filter(
    //   (course: any) => !courseSet.has(course.name.toLowerCase())
    // );
    // // Log to console
    // console.log(`🚨 Courses NOT in courseList: ${notInCourseList.length}`);
    // console.log(notInCourseList);

    // ✅ Write to file
    // const logFilePath = path.join(__dirname, "unmatched_courses.json");
    // fs.writeFileSync(
    //   logFilePath,
    //   JSON.stringify(notInCourseList, null, 2),
    //   "utf-8"
    // );
    // console.log(`📁 Unmatched courses saved to: ${logFilePath}`);

    // const notInCourseList = [
    //   "do_21430769643302092812498",
    //   "do_21430769639463321612495",
    //   "do_21430769645046169612501",
    //   "do_21430769659446067212507",
    // ];

    // console.log(notInCourseList);
    for (const course_do_id of courseIdentifiers) {
      this.logger.log(`🛠 Fixing course: (${course_do_id})`);

      try {
        const response = await this.getHierarchyUsingDoId(course_do_id);

        const { content } = response;

        if (!content) {
          this.logger.warn(`⚠️ Skipping ${course_do_id} — content not found`);
          continue;
        }

        const beforeProgram = Array.isArray(content.program)
          ? [...content.program]
          : [];

        console.log(content.program);
        if (Array.isArray(content.program)) {
          const programs = content.program;

          if (programs.length === 1 && programs[0] === "Vocational Training") {
            content.program = ["Open School"];
          } else if (programs.includes("Vocational Training")) {
            content.program = programs.filter(
              (p: any) => p !== "Vocational Training"
            );
          }

          console.log("Updated program:", content.program);
        }

        const afterProgram = Array.isArray(content.program)
          ? [...content.program]
          : [];

        // Skip if program is same
        if (
          beforeProgram.length === afterProgram.length &&
          beforeProgram.every((val, idx) => val === afterProgram[idx])
        ) {
          this.logger.log(
            `⏩ No program change for ${course_do_id}, skipping update`
          );
          continue;
        }

        const nodesModified = this.buildNodesModified(content, content.program);

        // 🔁 Build full hierarchy recursively from content.children
        const hierarchy = this.buildHierarchy(content);

        const payload = {
          request: {
            data: {
              nodesModified,
              hierarchy,
              lastUpdatedBy: this.configService.get("CREATED_BY"),
            },
          },
        };

        const result = await this.updateHierarchyUsingDoId(
          course_do_id,
          payload
        );

        const userToken = this.configService.get("ACCESS_TOKEN");
        await this.retryRequest(
          () => this.reviewContent(course_do_id, userToken),
          3,
          2000,
          "Review Content"
        );
        await this.retryRequest(
          () => this.publishContent(course_do_id, userToken),
          3,
          2000,
          "Publish Content"
        );
        this.logger.log(
          `✅ Review and publish completed for course: ${course_do_id}`
        );

        this.logger.log(`✅ Updated program for(${course_do_id})`);
      } catch (error: any) {
        this.logger.log(
          `❌ Failed to update ${course_do_id}:`,
          error?.message || error
        );
      }
    }

    console.log("✅ Completed taxonomy mapping for courses");
  }

  async findCourseByTitle(course: any): Promise<any> {
    try {
      let courseMetadata: CourseMetadataType = {};

      if (course.course_metadata) {
        if (typeof course.course_metadata === "string") {
          try {
            courseMetadata = JSON.parse(course.course_metadata);
          } catch (e) {
            this.logger.error(
              `❗ Invalid JSON in course_metadata for course: ${course.course_title}`
            );
            throw e;
          }
        } else {
          courseMetadata = course.course_metadata;
        }
      }

      const userToken = this.configService.get("ACCESS_TOKEN"); // Replace with dynamic token if needed
      const headers = {
        Authorization: `Bearer ${userToken}`,
        "Content-Type": "application/json",
        Accept: "application/json, text/plain, */*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "X-Channel-Id": this.configService.get("CREATED_FOR") || "",
        tenantid: this.configService.get("TENANT_ID"), // replace key name with actual env var
      };

      const url = `${this.configService.get(
        "MIDDLEWARE_QA"
      )}/action/composite/v3/search`;

      const toProgramArray = (value?: string | string[]): string[] => {
        if (!value || (typeof value === "string" && !value.trim())) {
          return ["Open School"];
        }

        if (Array.isArray(value)) {
          return value.length
            ? value.map((item) => item.trim())
            : ["Open School"];
        }

        return value.includes(",")
          ? value.split(",").map((item) => item.trim())
          : [value.trim()];
      };

      const requestBody = {
        request: {
          filters: {
            status: [
              "Draft",
              "FlagDraft",
              "Review",
              "Processing",
              "Live",
              "Unlisted",
              "FlagReview",
            ],
            program: toProgramArray(courseMetadata.program),
            domain: [courseMetadata.domain || "Learning for Life"],
            primaryUser: courseMetadata.primary_user || "",
            se_subDomains: [courseMetadata.sub_domain || ""],
            se_subjects: courseMetadata.subjects || "",
            contentLanguage: courseMetadata.content_language || "",
            primaryCategory: ["Course"],
            channel: "pos-channel",
          },
          sort_by: { lastUpdatedOn: "desc" },
          query: course.course_title,
          limit: 1,
          offset: 0,
        },
      };

      this.logger.log(`📦 Fetching published courses: ${url}`);

      const response = await this.httpService
        .post(url, requestBody, { headers })
        .toPromise();

      this.logger.log(
        "✅ Fetched Published Courses:",
        response.data.result?.content?.length || 0
      );

      const count = Number(response?.data?.result?.count ?? 0);
      const contentArr = Array.isArray(response?.data?.result?.content)
        ? response?.data.result.content
        : [];

      if (contentArr.length === 0) {
        return { count, identifier: null };
      }

      const first = contentArr[0];

      // Prefer string 'identifier' like 'do_2144...' (most useful for storing)
      if (first.identifier && typeof first.identifier === "string") {
        return { count, identifier: first.identifier };
      }
    } catch (error) {
      this.logger.error(
        error instanceof Error ? error.message : JSON.stringify(error)
      );
      throw error;
    }
  }

  async getPublishedCourses(limit = 1): Promise<any> {
    try {
      const userToken = this.configService.get("ACCESS_TOKEN"); // Replace with dynamic token if needed
      const headers = {
        Authorization: `Bearer ${userToken}`,
        "Content-Type": "application/json",
        Accept: "application/json, text/plain, */*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "X-Channel-Id": this.configService.get("CREATED_FOR") || "",
        tenantid: this.configService.get("TENANT_ID"), // replace key name with actual env var
      };

      const url = `${this.configService.get(
        "MIDDLEWARE_QA"
      )}/action/composite/v3/search`;

      const requestBody = {
        request: {
          filters: {
            status: [
              // "Draft",
              // "FlagDraft",
              // "Review",
              // "Processing",
              "Live",
              // "Unlisted",
              // "FlagReview",
            ],
            program: ["Open School"],
            // createdBy: {
            //   "!=": this.configService.get("CREATED_BY"),
            // },
            primaryCategory: ["Course"],
            channel: "pos-channel",
            // program: ["Vocational Training"],
          },
          sort_by: { lastUpdatedOn: "desc" },
          // query: "do_214406640381419520133",
          limit: limit,
          offset: 0,
        },
      };

      this.logger.log(`📦 Fetching published courses: ${url}`);

      const response = await this.httpService
        .post(url, requestBody, { headers })
        .toPromise();

      this.logger.log(
        "✅ Fetched Published Courses:",
        response.data.result?.content?.length || 0
      );

      return response.data.result || {};
    } catch (error) {
      this.logger.error(
        error instanceof Error ? error.message : JSON.stringify(error)
      );
      throw error;
    }
  }

  async FinancialLiteracyCourseVocationalTrainingTag(limit = 20) {
    const fetched = await this.getPublishedCourses(limit);

    // Normalize fetched course names and include identifier
    const fetchedCourses =
      fetched?.content?.map((course: any) => ({
        name: course.name?.trim(),
        identifier: course.identifier,
        program: course.program,
      })) || [];

    // Build a Set for faster lookup (optional but recommended)

    // Filter courses NOT in courseList
    const notInCourseList = [];
    // Log to console
    console.log(`🚨 Courses NOT in courseList: ${fetchedCourses.length}`);
    console.log(fetchedCourses);

    // ✅ Write to file
    const logFilePath = path.join(
      __dirname,
      "FinancialLiteracyCourseVocationalTrainingTag.json"
    );
    fs.writeFileSync(
      logFilePath,
      JSON.stringify(fetchedCourses, null, 2),
      "utf-8"
    );
    console.log(`📁 Unmatched courses saved to: ${logFilePath}`);

    console.log(`✅ Total courses fetched: ${fetchedCourses.length}`);
    console.log(fetchedCourses);
    for (const updateCourse of fetchedCourses) {
      const course_do_id = updateCourse.identifier;
      this.logger.log(`🛠 Fixing course: (${course_do_id})`);

      try {
        const response = await this.getHierarchyUsingDoId(course_do_id);

        const { content } = response;

        if (!content) {
          this.logger.warn(`⚠️ Skipping ${course_do_id} — content not found`);
          continue;
        }

        const beforeProgram = Array.isArray(content.program)
          ? [...content.program]
          : [];

        console.log(content.program);

        if (Array.isArray(content.program)) {
          const programs = content.program;

          // If "Vocational Training" already exists → skip
          if (programs.includes("Vocational Training")) {
            this.logger.log(
              `⏩ "Vocational Training" already exists for ${course_do_id}, skipping update`
            );
            continue;
          }

          // If missing → add it (or run your processing logic)
          programs.push("Vocational Training");
          content.program = programs;

          console.log("Updated program:", content.program);
        }

        const afterProgram = Array.isArray(content.program)
          ? [...content.program]
          : [];

        // Skip if no change (safety check)
        if (
          beforeProgram.length === afterProgram.length &&
          beforeProgram.every((val, idx) => val === afterProgram[idx])
        ) {
          this.logger.log(
            `⏩ No program change for ${course_do_id}, skipping update`
          );
          continue;
        }

        const nodesModified = this.buildNodesModified(content, content.program);

        // 🔁 Build full hierarchy recursively from content.children
        const hierarchy = this.buildHierarchy(content);

        const payload = {
          request: {
            data: {
              nodesModified,
              hierarchy,
              lastUpdatedBy: this.configService.get("CREATED_BY"),
            },
          },
        };

        const result = await this.updateHierarchyUsingDoId(
          course_do_id,
          payload
        );

        const userToken = this.configService.get("ACCESS_TOKEN");
        await this.retryRequest(
          () => this.reviewContent(course_do_id, userToken),
          3,
          2000,
          "Review Content"
        );
        await this.retryRequest(
          () => this.publishContent(course_do_id, userToken),
          3,
          2000,
          "Publish Content"
        );
        this.logger.log(
          `✅ Review and publish completed for course: ${course_do_id}`
        );

        this.logger.log(`✅ Updated program for(${course_do_id})`);
      } catch (error: any) {
        this.logger.log(
          `❌ Failed to update ${course_do_id}:`,
          error?.message || error
        );
      }
    }

    console.log("✅ Completed taxonomy mapping for courses");
  }

  async updateRecordsChangeCreatedBy(limit = 21) {
    const fetched = await this.getPublishedCourses(limit);

    // Normalize fetched course names and include identifier
    const fetchedCourses =
      fetched?.content?.map((course: any) => ({
        name: course.name?.trim(),
        identifier: course.identifier,
        program: course.program,
      })) || [];

    // Build a Set for faster lookup (optional but recommended)

    // Filter courses NOT in courseList
    const notInCourseList = [];
    // Log to console
    console.log(`🚨 Courses NOT in courseList: ${fetchedCourses.length}`);
    console.log(fetchedCourses);

    // ✅ Write to file
    const logFilePath = path.join(
      __dirname,
      "FinancialLiteracyCourseVocationalTrainingTag.json"
    );
    fs.writeFileSync(
      logFilePath,
      JSON.stringify(fetchedCourses, null, 2),
      "utf-8"
    );
    console.log(`📁 Unmatched courses saved to: ${logFilePath}`);

    console.log(`✅ Total courses fetched: ${fetchedCourses.length}`);
    console.log(fetchedCourses);
    for (const updateCourse of fetchedCourses) {
      const course_do_id = updateCourse.identifier;
      this.logger.log(`🛠 Fixing course: (${course_do_id})`);

      try {
        const response = await this.getHierarchyUsingDoId(course_do_id);

        const { content } = response;

        if (!content) {
          this.logger.warn(`⚠️ Skipping ${course_do_id} — content not found`);
          continue;
        }

        const nodesModified = this.buildNodesModified(
          content,
          content.program,
          true
        );

        // 🔁 Build full hierarchy recursively from content.children
        const hierarchy = this.buildHierarchy(content);

        const payload = {
          request: {
            data: {
              nodesModified,
              hierarchy,
              lastUpdatedBy: this.configService.get("CREATED_BY"),
            },
          },
        };

        const result = await this.updateHierarchyUsingDoId(
          course_do_id,
          payload
        );

        const userToken = this.configService.get("ACCESS_TOKEN");
        await this.retryRequest(
          () => this.reviewContent(course_do_id, userToken),
          3,
          2000,
          "Review Content"
        );
        await this.retryRequest(
          () => this.publishContent(course_do_id, userToken),
          3,
          2000,
          "Publish Content"
        );
        this.logger.log(
          `✅ Review and publish completed for course: ${course_do_id}`
        );

        this.logger.log(`✅ Updated program for(${course_do_id})`);
      } catch (error: any) {
        this.logger.log(
          `❌ Failed to update ${course_do_id}:`,
          error?.message || error
        );
      }
    }

    console.log("✅ Completed taxonomy mapping for courses");
  }


  async updateRecordsChangeIcon(limit = 2100) {
    try {
      const published = await this.getPublishedCourses(limit);
      const courses = Array.isArray(published?.content) ? published.content : [];

      if (!courses.length) {
        this.logger.log("✅ No published courses found for icon update.");
        return;
      }
      console.log(courses);

      for (const course of courses) {
        const course_do_id: string | undefined = course?.identifier;
        if (!course_do_id) continue;

        try {
          // Fetch full hierarchy to get metadata fields (domain/subdomain/subject)
          const response = await this.getHierarchyUsingDoId(course_do_id);
          const { content } = response;
          if (!content) {
            this.logger.warn(`⚠️ Skipping ${course_do_id} — content not found`);
            continue;
          }

          // Extract taxonomy values from content (as per provided console sample)
          const normalizeToArray = (val: any): string[] => {
            if (!val) return [];
            if (Array.isArray(val)) return val.filter(Boolean);
            if (typeof val === "string") return val.trim() ? [val.trim()] : [];
            return [];
          };

          const domains = normalizeToArray((content as any).se_domains);
          const subDomains = normalizeToArray((content as any).se_subDomains);
          const subjects = normalizeToArray((content as any).se_subjects);

          if (domains.length === 0) {
            this.logger.warn(`⚠️ No domain found for ${course_do_id}. Skipping icon update.`);
            continue;
          }

          // Build dynamic query to fetch a random matching thumbnail
          const whereParts: string[] = [];
          const params: any[] = [];

          // domain match (any of the course domains)
          if (domains.length === 1) {
            whereParts.push(`domain = ?`);
            params.push(domains[0]);
          } else {
            whereParts.push(`domain IN (${domains.map(() => "?").join(",")})`);
            params.push(...domains);
          }

          // subdomain match if present
          if (subDomains.length > 0) {
            if (subDomains.length === 1) {
              whereParts.push(`subdomain = ?`);
              params.push(subDomains[0]);
            } else {
              whereParts.push(`subdomain IN (${subDomains.map(() => "?").join(",")})`);
              params.push(...subDomains);
            }
          }

          // subject match if present
          if (subjects.length > 0) {
            if (subjects.length === 1) {
              whereParts.push(`subject = ?`);
              params.push(subjects[0]);
            } else {
              whereParts.push(`subject IN (${subjects.map(() => "?").join(",")})`);
              params.push(...subjects);
            }
          }

          const whereClause = whereParts.length ? `WHERE ${whereParts.join(" AND ")}` : "";
          const thumbQuery = `SELECT id, domain, subdomain, subject, thumbnail_name, name, link, folder FROM thumbnails ${whereClause} ORDER BY RAND() LIMIT 1`;

          const matchRows = await this.courseRepository.query(thumbQuery, params);
          if (!Array.isArray(matchRows) || matchRows.length === 0) {
            this.logger.warn(`⚠️ No matching thumbnail found for ${course_do_id}.`);
            continue;
          }

          // Build hierarchy and find all non-leaf nodes that are missing appIcon
          const hierarchy = this.buildHierarchy(content);
          const rootId = content.identifier;
          const nodeIdsToUpdate: string[] = [];
          const traverseForMissingIcons = (node: any) => {
            if (!node || !node.identifier) return;
            const hasChildren = Array.isArray(node.children) && node.children.length > 0;
            if (hasChildren) {
              const iconVal = typeof node.appIcon === "string" ? node.appIcon.trim() : "";
              if (!iconVal) nodeIdsToUpdate.push(node.identifier);
              for (const child of node.children) traverseForMissingIcons(child);
            }
          };
          traverseForMissingIcons(content);

          const nodesModified: Record<string, any> = {};

          for (const nodeId of nodeIdsToUpdate) {
            // pick random thumbnail row for each node
            const matchForNode = await this.courseRepository.query(thumbQuery, params);
            if (!Array.isArray(matchForNode) || matchForNode.length === 0) {
              this.logger.warn(`⚠️ No matching thumbnail found for node ${nodeId} of course ${course_do_id}.`);
              continue;
            }

            let iconSourceUrl: string | undefined = matchForNode[0]?.link;
            if (!iconSourceUrl) {
              this.logger.warn(`⚠️ Matching record has no link for node ${nodeId}.`);
              continue;
            }

            // Convert Google Drive share links to direct download URLs
            const driveMatch = iconSourceUrl.match(/https?:\/\/drive\.google\.com\/file\/d\/([^/]+)\//);
            if (driveMatch && driveMatch[1]) {
              iconSourceUrl = `https://drive.google.com/uc?export=download&id=${driveMatch[1]}`;
            }

            const uploadedUrl = await this.uploadIcon(
              iconSourceUrl,
              this.configService.get("ACCESS_TOKEN"),
              this.configService.get("CREATED_BY") || ""
            );

            if (!uploadedUrl || typeof uploadedUrl !== "string") {
              this.logger.warn(`⚠️ Upload failed for node ${nodeId} in course ${course_do_id}. Skipping.`);
              continue;
            }

            nodesModified[nodeId] = {
              metadata: { appIcon: uploadedUrl },
              isNew: false,
              objectType: nodeId === rootId ? "Content" : "Collection",
              root: nodeId === rootId,
            };
          }

          if (Object.keys(nodesModified).length === 0) {
            this.logger.warn(`⚠️ No nodes updated for course ${course_do_id}. Skipping review/publish.`);
            continue;
          }

          const payload = {
            request: {
              data: {
                nodesModified,
                hierarchy,
                lastUpdatedBy: this.configService.get("CREATED_BY"),
              },
            },
          };

          await this.updateHierarchyUsingDoId(course_do_id, payload);

          // Review and publish
          const userToken = this.configService.get("ACCESS_TOKEN");
          await this.retryRequest(
            () => this.reviewContent(course_do_id, userToken),
            3,
            2000,
            "Review Content"
          );
          await this.retryRequest(
            () => this.publishContent(course_do_id, userToken),
            3,
            2000,
            "Publish Content"
          );

          this.logger.log(`✅ Icon updated, reviewed, and published for course: ${course_do_id}`);
        } catch (err: any) {
          this.logger.warn(
            `❌ Failed icon update for ${course_do_id}: ${err?.message || err}`
          );
        }
      }
    } catch (error: any) {
      this.logger.error(`❌ updateRecordsChangeIcon failed: ${error?.message || error}`);
      throw error;
    }
  }
}
