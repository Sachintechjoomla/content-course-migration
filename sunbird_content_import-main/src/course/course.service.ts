import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';

@Injectable()
export class CourseService {
  private courses = []; // Temporary in-memory store
  private readonly middlewareUrl: string;

  constructor(private readonly configService: ConfigService) {
    this.middlewareUrl = this.configService.get<string>('MIDDLEWARE_URL') || '';
  }

  async createCourse(name: string, description: string): Promise<any> {
  
    const { v4: uuidv4 } = require('uuid');
    const uniqueCode = uuidv4();

    const payload = {
      request: {
        content: {
          name: name,
          code: uniqueCode, // Generate a unique code dynamically
          mimeType: 'application/vnd.ekstep.content-collection',
          contentType: "Course",
          primaryCategory: 'Course',
          createdBy: this.configService.get<string>('USER_ID'),
          createdFor: [
            this.configService.get<string>('X_CHANNEL_ID')
        ],
        },
      },
    };

    const payloadString = JSON.stringify(payload); // Convert payload to a JSON string
    const contentLength = Buffer.byteLength(payloadString, 'utf8'); // Calculate byte size      

    const headers = {
      "Content-Type": "application/json",
      "Content-Length":contentLength,
      "tenantId": this.configService.get<string>('MIDDLEWARE_TENANT_ID'), // Replace with dynamic or environment value
      Authorization: `Bearer ${this.configService.get<string>('USER_TOKEN')}`,
      "X-Channel-Id":this.configService.get<string>('X_CHANNEL_ID')
    };

    
    const createResponse = await axios.post(`${this.middlewareUrl}/action/content/v3/create`, payload, {
      headers,
    });

    console.log(createResponse);


  }



  
  async getCourseByName(name: string): Promise<any> {

      const payload = {
        request: {
            filters: {
                status: [
                    "Draft",
                    "FlagDraft",
                    "Review",
                    "Processing",
                    "Live",
                    "Unlisted",
                    "FlagReview"
                ],
                primaryCategory: [
                    "Content Playlist",
                    "Course",
                    "Digital Textbook",
                    "Question paper",
                    "Course Assessment",
                    "eTextbook",
                    "Explanation Content",
                    "Learning Resource",
                    "Practice Question Set",
                    "Teacher Resource",
                    "Exam Question"
                ]
            },
            sort_by: {
                lastUpdatedOn: "desc"
            },
            query: name
        }
    };

    const payloadString = JSON.stringify(payload); // Convert payload to a JSON string
    const contentLength = Buffer.byteLength(payloadString, 'utf8'); // Calculate byte size      

    const headers = {
      "Content-Type": "application/json",
      "Content-Length":contentLength,
      "tenantId": this.configService.get<string>('MIDDLEWARE_TENANT_ID'), // Replace with dynamic or environment value
      Authorization: `Bearer ${this.configService.get<string>('USER_TOKEN')}`,
      "X-Channel-Id":this.configService.get<string>('X_CHANNEL_ID')
    };

    
    
    const courseDetails = await axios.post(`${this.middlewareUrl}/action/composite/v3/search`, payload, {
      headers,
    });

  // Extract required fields from response
  const { count, content } = courseDetails.data.result;

  return { count, content };

   // return this.courses.find(course => course.name === name) || null;
  }


}
