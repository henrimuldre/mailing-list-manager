/**
 * PM2 process definition for the admin UI.
 * 
 *   cd list-admin
 *   pm2 start ecosystem.config.js
 * 
 * If your host manages PM2 apps from its web panel, point the panel at app.py with the interpreter below, or import this file if the panel supports it.
 * Paths are resolved relative to this file, so no editing is needed.
 */
const path = require("path");

module.exports = {
    apps: [
        {
            name: "list-admin",
            script: path.join(__dirname, "app.py"),
            cwd: __dirname,
            interpreter: path.join(__dirname, ".venv", "bin", "python"),
            args: ["--no-debug", "--no-reload", "--no-debugger"],
            autorestart: true,
            env: {
                PYTHONUNBUFFERED: "1",
            },
        },
    ],
};
