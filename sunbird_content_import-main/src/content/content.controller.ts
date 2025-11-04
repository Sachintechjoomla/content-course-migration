import { Controller, Post, UploadedFile, UseInterceptors, Body } from '@nestjs/common';
import { ApiBody, ApiConsumes, ApiOperation, ApiTags } from '@nestjs/swagger';
import { FileInterceptor } from '@nestjs/platform-express';
import { ContentService } from './content.service';

@ApiTags('Content')
@Controller('content')
export class ContentController {
  constructor(private readonly contentService: ContentService) {}

  @Post('bulk-create')
  @UseInterceptors(
    FileInterceptor('file', {
      limits: {
        fileSize: 300 * 1024 * 1024, // ✅ Increased to 300MB
      },
    }),
  )
  @ApiOperation({ summary: 'Bulk create content from CSV' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        file: {
          type: 'string',
          format: 'binary',
          description: 'The CSV file containing Title and FileUrl columns',
        },
        userId: {
          type: 'string',
          example: '15155b7a-5316-4bb2-992a-772093e85f44',
          description: 'The ID of the user creating the content',
        },
        userToken: {
          type: 'string',
          example: 'eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJPc3NtSUhXaW1NMDN2MUxsVnFvNHBqaS0ydEMwTGhLY0o5dmtwQTlJZV9zIn0.eyJleHAiOjE3MzQ1MDYyNTcsImlhdCI6MTczNDQxOTg1NywianRpIjoiZGVlYmQzODAtYWM5Mi00ODQ0LTlmODctNDE0ZjA4ZjljNTBiIiwiaXNzIjoiaHR0cHM6Ly9xYS5wcmF0aGFtdGVhY2hlcmFwcC50ZWtkaW5leHQuY29tL2F1dGgvcmVhbG1zL3ByYXRoYW0iLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiMTUxNTViN2EtNTMxNi00YmIyLTk5MmEtNzcyMDkzZTg1ZjQ0IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoicHJhdGhhbSIsInNlc3Npb25fc3RhdGUiOiI3NmRiNzQ1Yi03MmM2LTQxYjAtODVhMS1hZjBmMGM0YjNjODAiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIi8qIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIiwiZGVmYXVsdC1yb2xlcy1wcmF0aGFtIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJlbWFpbCBwcm9maWxlIiwic2lkIjoiNzZkYjc0NWItNzJjNi00MWIwLTg1YTEtYWYwZjBjNGIzYzgwIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJuYW1lIjoiU2h1YmhhbSIsInByZWZlcnJlZF91c2VybmFtZSI6InNodWJoYW1qOTg3NjUiLCJnaXZlbl9uYW1lIjoiU2h1YmhhbSIsImZhbWlseV9uYW1lIjoiIn0.sXx1wp39xp3n9a4SphOph2ZBjPxhuJWc8LOnQa7z4c7iicFMJSxsRF9bo315txKg7nEub0gQqrehbO-3eAynN7dR-eKZ7giFD1WNi95XBHM-9cwyjbQ9clt6Tl4yNHbFxU_5Qn8LszWleJnVids4vMEn6liY8BVPuie8w65BXYLqV6SVlsj0OLX-PnelveIrJNAFGR17Gdda_zfdUI_oKsAPqc5n4DK_JIM67yLdOb6aIWgij_4Vtnfsm4itux1_AqrTfCzt5L4OeDssB08aGGNKrr-jcgQxlkM1Jm_9EF36I6ZTc6TnG1hBvz2VciJAzazI48V0W_g6f_1A5T52NA',
          description: 'User Token for creating content. This user should have access to create content.',
        },
      },
    },
  })
  async bulkCreateContent(
    @UploadedFile() file: Express.Multer.File,
    @Body('userId') userId: string,
    @Body('userToken') userToken: string,
  ) {
    //return this.contentService.processCsvAndCreateContent(file, userId, userToken);
  }

  
}
