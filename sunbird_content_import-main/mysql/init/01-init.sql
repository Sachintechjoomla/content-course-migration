-- MySQL initialization script for Sunbird Content Import Service
-- This script creates the database schema and initial data

USE node-contents;

-- Create the hindi table (content entity)
CREATE TABLE IF NOT EXISTS `hindi` (
  `id` char(36) NOT NULL,
  `program` varchar(255) DEFAULT NULL,
  `domain` varchar(255) DEFAULT NULL,
  `sub_domain` varchar(255) DEFAULT NULL,
  `content_language` varchar(50) DEFAULT NULL,
  `primary_user` varchar(255) DEFAULT NULL,
  `target_age_group` varchar(100) DEFAULT NULL,
  `cont_title` varchar(255) DEFAULT NULL,
  `cont_engtitle` varchar(255) DEFAULT NULL,
  `cont_url` text DEFAULT NULL,
  `cont_dwurl` text DEFAULT NULL,
  `type` text DEFAULT NULL,
  `resource_desc` text DEFAULT NULL,
  `subjects` text DEFAULT NULL,
  `course_keywords` text DEFAULT NULL,
  `migrated` tinyint(1) DEFAULT 0,
  `do_id` varchar(1000) DEFAULT NULL,
  `convertedFileflag` tinyint(1) DEFAULT 0,
  `convertedUrl` text DEFAULT NULL,
  `old_system_content_id` varchar(255) DEFAULT NULL,
  `comment` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_migrated ON `hindi` (`migrated`);
CREATE INDEX IF NOT EXISTS idx_do_id ON `hindi` (`do_id`);
CREATE INDEX IF NOT EXISTS idx_old_system_content_id ON `hindi` (`old_system_content_id`);

-- Insert some sample data (optional)
INSERT IGNORE INTO `hindi` (`id`, `program`, `domain`, `cont_title`, `migrated`) VALUES
('sample-001', 'Sample Program', 'Sample Domain', 'Sample Content Title', 0),
('sample-002', 'Test Program', 'Test Domain', 'Test Content Title', 0);

-- Create additional tables if needed for the application
-- Add any other tables that might be required by your application

-- Grant permissions to the application user
GRANT ALL PRIVILEGES ON `node-contents`.* TO 'sunbird_user'@'%';
FLUSH PRIVILEGES;

-- Show the created table structure
DESCRIBE `hindi`; 