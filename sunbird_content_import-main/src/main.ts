import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import express from 'express'; // ✅ Import express

async function bootstrap() {
  // 🚫 Disable Nest’s internal bodyParser
  const app = await NestFactory.create(AppModule, {
    bodyParser: false,
    logger: ['log', 'error', 'warn', 'debug', 'verbose'],
  });

  const configService = app.get(ConfigService);

  // ✅ Manually set larger body size limits
  app.use(express.json({ limit: '600mb' }));
  app.use(express.urlencoded({ limit: '600mb', extended: true }));

  // Swagger setup
  const config = new DocumentBuilder()
    .setTitle('Content Service API')
    .setDescription('API for bulk content creation')
    .setVersion('1.0')
    .build();
  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api', app, document);

  const port = configService.get<number>('port') || 3001;
  await app.listen(port);
  console.log(`Application is running on: http://localhost:${port}`);
  console.log(`Swagger is available at: http://localhost:${port}/api`);
}

bootstrap();

