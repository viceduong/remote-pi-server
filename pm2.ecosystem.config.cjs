/**
 * pm2 ecosystem — Remote Pi bridge server.
 *
 *   pm2 start pm2.ecosystem.config.cjs && pm2 save
 *   (Linux/macOS: pm2 startup; Windows: use NSSM — see scripts/)
 */
module.exports = {
  apps: [
    {
      name: 'remote-pi-server',
      script: 'dist/index.js',
      cwd: __dirname,
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_restarts: 20,
      restart_delay: 2000,
      watch: false,
      time: true,
      merge_logs: true,
      out_file: 'logs/pm2-out.log',
      error_file: 'logs/pm2-err.log',
      env: {
        NODE_ENV: 'production',
        REMOTE_PI_PORT: '8787',
        // REMOTE_PI_TOKEN: 'set-me',
        // REMOTE_PI_WORKDIR: '/path/to/projects',
      },
    },
  ],
};
