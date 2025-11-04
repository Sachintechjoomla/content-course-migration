import { Module } from '@nestjs/common';
import { ContentController } from './content.controller';
import { ContentService } from './content.service';
import { CourseModule } from '../course/course.module';
import { ConfigModule } from '@nestjs/config';
import { FileLoggerService } from '../logger/file-logger.service';

@Module({
  imports: [CourseModule, ConfigModule], // Import ConfigModule
  controllers: [ContentController],
  providers: [ContentService, FileLoggerService],
  exports: [ContentService],
})
export class ContentModule {}
