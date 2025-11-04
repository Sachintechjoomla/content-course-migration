import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';  // ✅ Import ConfigModule
import { TypeOrmModule } from '@nestjs/typeorm';  // ✅ Import TypeOrmModule
import { HttpModule } from '@nestjs/axios';  // ✅ Import HttpModule
import { CourseService } from './course.service';  // ✅ Import CourseService
import { CourseController } from './course.controller';  // ✅ Import CourseController
import { Course } from './course.entity';  // ✅ Import Course Entity

@Module({
  imports: [
    ConfigModule,  // ✅ Correct usage without .forRoot()
    TypeOrmModule.forFeature([Course]),
    HttpModule
  ],
  providers: [CourseService],
  controllers: [CourseController],
})
export class CourseModule {}
