import { Controller, Post, Param, Body } from "@nestjs/common";
import { CourseService } from "./course.service";

@Controller("courses")
export class CourseController {
  constructor(private readonly courseService: CourseService) {}

  @Post("import")
  async importCourses() {
   // console.log("Importing courses...");
   return this.courseService.importCourses();
  }
  @Post("course-issue-fixes")
  async CourseIssueFixes() {
   // console.log("Importing courses...");
   return this.courseService.CourseIssueFixes();
  }

  @Post("delete-all")
  async deleteAllCourses() {
   // console.log("Importing courses...");
   return this.courseService.deleteAllCourses();
  }

  @Post("taxonomy-Vocational-Training-Courses-issues")
  async taxonomyMappingVocationalTrainingCourses() {
   return this.courseService.taxonomyMappingVocationalTrainingCourses();
  }

  @Post("financial-Literacy-Course-Vocational-Training-Tag")
  async FinancialLiteracyCourseVocationalTrainingTag() {
   return this.courseService.FinancialLiteracyCourseVocationalTrainingTag();
  }

  @Post("updateRecordsChangeCreatedBy")
  async updateRecordsChangeCreatedBy() {
   return this.courseService.updateRecordsChangeCreatedBy();
  }

  @Post("updateRecordsChangeIcon")
  async updateRecordsChangeIcon() {
   return this.courseService.updateRecordsChangeIcon();
  }
}
