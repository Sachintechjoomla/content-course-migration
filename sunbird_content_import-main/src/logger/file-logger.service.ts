// file-logger.service.ts
import { LoggerService } from '@nestjs/common';

export class FileLoggerService implements LoggerService {
  log(message: string) {
    console.log(`[LOG]: ${message}`);
  }

  error(message: string, trace: string) {
    console.error(`[ERROR]: ${message}`, trace);
  }

  warn(message: string) {
    console.warn(`[WARN]: ${message}`);
  }
}
