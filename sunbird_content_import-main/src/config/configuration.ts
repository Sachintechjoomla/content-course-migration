export default () => ({
  middlewareUrl: process.env.MIDDLEWARE_URL || '',
  port: parseInt(process.env.PORT || '3000', 10),
});