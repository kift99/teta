import 'dotenv/config'; // Load environment variables from .env file
import nodemailer from 'nodemailer'; // Library for sending emails
import fs from 'fs'; // Node.js file system module
import { createReadStream } from 'fs'; // For reading files streamingly
import { createInterface } from 'readline'; // For reading files line by line
import EmailValidator from 'email-validator'; // Library for basic email format validation
import winston from 'winston'; // Library for logging
import chalk from 'chalk'; // Library for colorful console output
import cliProgress from 'cli-progress'; // Library for displaying progress bars in the CLI
import fastcsv from 'fast-csv'; // Library for efficiently writing CSV files
import dns from 'dns'; // Node.js DNS module for MX record lookups
import { promisify } from 'util'; // Utility to convert callback-based functions to Promise-based
import path from 'path'; // Node.js path module for handling file paths

// Promisify DNS methods for async/await usage
const resolveMx = promisify(dns.resolveMx);
const dnsLookup = promisify(dns.lookup);

// --- Node.js Version Check ---
const requiredNodeVersion = '16.0.0'; // Minimum required Node.js version
const currentNodeVersion = process.version.substr(1); // Get current Node.js version (removing 'v')

// Helper function to parse version strings (e.g., "16.0.0") into arrays of numbers ([16, 0, 0])
function parseVersion(versionString) {
    return versionString.split('.').map(Number);
}

// Helper function to compare two version arrays
function compareVersions(currentVersion, requiredVersion) {
    const currentParsed = parseVersion(currentVersion);
    const requiredParsed = parseVersion(requiredVersion);

    for (let i = 0; i < Math.max(currentParsed.length, requiredParsed.length); i++) {
        const currentPart = currentParsed[i] || 0; // Default to 0 if part doesn't exist
        const requiredPart = requiredParsed[i] || 0;

        if (currentPart < requiredPart) return -1; // Current is older
        if (currentPart > requiredPart) return 1;  // Current is newer
    }
    return 0; // Versions are equal
}

// Exit if Node.js version is too old
if (compareVersions(currentNodeVersion, requiredNodeVersion) < 0) {
    console.error(chalk.red(`❌ Node.js version ${currentNodeVersion} is not supported. Please use Node.js ${requiredNodeVersion} or higher.`));
    process.exit(1); // Exit with error code
}
console.log(chalk.blue(`✅ Node.js version ${currentNodeVersion} is supported.`));

// --- Configuration Loading & Validation ---
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE, 10) || 15; // Number of emails to process in parallel conceptually (limited by rate limiter)
const DELAY_BETWEEN_BATCHES = parseInt(process.env.DELAY_BETWEEN_BATCHES, 10) || 100; // Milliseconds to wait between processing conceptual batches
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES, 10) || 2; // Max attempts for sending an email after initial failure
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY, 10) || 500; // Base delay (ms) before the first retry (increases exponentially)
const SMTP_TEST = process.env.SMTP_TEST === 'true'; // If true, simulate sending without actually sending emails
const SKIP_SMTP_VERIFY = process.env.SKIP_SMTP_VERIFY !== 'false'; // Skip SMTP verification by default (faster, avoids issues with some servers)
const SKIP_DNS_FAILURES = process.env.SKIP_DNS_FAILURES === 'true'; // If true, queue emails with 451 DNS errors for retry at the end

// Validate required environment variables
const validateConfig = () => {
    const requiredEnvVars = ['SMTP_CONFIG_PATH', 'RECIPIENT_LIST_PATH', 'ATTACHMENT_PATH', 'MESSAGE_FILE_PATH'];
    const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);
    if (missingVars.length > 0) {
        console.error(chalk.red(`❌ Missing required environment variables: ${missingVars.join(', ')}`));
        process.exit(1);
    }
    // Check if files exist
    for (const varName of requiredEnvVars) {
        const filePath = process.env[varName];
         // Check if the file exists
        if (!fs.existsSync(filePath)) {
            console.error(chalk.red(`❌ File specified in ${varName} not found: ${filePath}`));
            process.exit(1);
        }
    }
    console.log(chalk.blue('✅ Environment variables and file paths verified.'));
};
validateConfig(); // Run validation


// --- Adaptive Load Control System ---
// Dynamically adjusts concurrency and batch size based on observed performance
const adaptiveControl = {
    // Configuration (can be overridden by .env)
    concurrency: parseInt(process.env.INITIAL_CONCURRENCY, 10) || 8, // Initial number of parallel sends allowed by rate limiter
    maxConcurrency: parseInt(process.env.MAX_CONCURRENCY, 10) || 20,
    minConcurrency: parseInt(process.env.MIN_CONCURRENCY, 10) || 3,
    batchSize: BATCH_SIZE, // Conceptual batch size (used for slicing recipient list)
    maxBatchSize: parseInt(process.env.MAX_BATCH_SIZE, 10) || 50,
    minBatchSize: parseInt(process.env.MIN_BATCH_SIZE, 10) || 5,

    // Performance Metrics
    successRate: 1.0, // Moving average of success rate (0.0 to 1.0)
    errorWindow: [], // Sliding window of recent outcomes (1 for success, 0 for failure)
    responseTimeWindow: [], // Sliding window of recent send times (ms)

    // Records the outcome of a send attempt
    registerResult(success, responseTimeMs) {
        // Update error window (keep last 50 results)
        this.errorWindow.push(success ? 1 : 0);
        if (this.errorWindow.length > 50) this.errorWindow.shift(); // Remove oldest entry

        // Update response time window (keep last 20)
        if (responseTimeMs) {
            this.responseTimeWindow.push(responseTimeMs);
            if (this.responseTimeWindow.length > 20) this.responseTimeWindow.shift();
        }

        // Recalculate success rate
        const sum = this.errorWindow.reduce((a, b) => a + b, 0);
        this.successRate = this.errorWindow.length > 0 ? sum / this.errorWindow.length : 1.0;

        // Adjust parameters every 20 results to avoid excessive fluctuation
        if (this.errorWindow.length > 0 && this.errorWindow.length % 20 === 0) {
            this.adjustParameters();
        }
    },

    // Adjusts concurrency and batch size based on current performance
    adjustParameters() {
        const oldConcurrency = this.concurrency;
        const oldBatchSize = this.batchSize;

        if (this.successRate > 0.95) { // High success rate: Increase limits
            this.concurrency = Math.min(this.concurrency + 1, this.maxConcurrency);
            this.batchSize = Math.min(this.batchSize + 2, this.maxBatchSize);
        } else if (this.successRate < 0.75) { // Low success rate: Decrease limits
            this.concurrency = Math.max(this.concurrency - 1, this.minConcurrency);
            this.batchSize = Math.max(this.batchSize - 2, this.minBatchSize);
        }
        // Note: For medium success rate (0.75-0.95), no automatic adjustment is made here,
        // relying on minor fluctuations or external factors. Could add logic based on response time if needed.

        // Log changes if they occurred
        if (this.concurrency !== oldConcurrency || this.batchSize !== oldBatchSize) {
             const direction = (this.concurrency > oldConcurrency || this.batchSize > oldBatchSize) ? chalk.green('🔼 Increasing') : chalk.yellow('🔽 Reducing');
             console.log(chalk.magenta(`${direction} limits - Concurrency: ${this.concurrency}, Batch Size: ${this.batchSize} (Success Rate: ${(this.successRate * 100).toFixed(1)}%)`));
        }

        // IMPORTANT: Update the actual rate limiter with the new concurrency
        rateLimiter.concurrency = this.concurrency;
    },

    // Calculates the average response time from the window
    getAverageResponseTime() {
        if (this.responseTimeWindow.length === 0) return 1000; // Default if no data
        return this.responseTimeWindow.reduce((a, b) => a + b, 0) / this.responseTimeWindow.length;
    },

    // Gets the current conceptual batch size
    getBatchSize() {
        return this.batchSize;
    },

    // Returns current stats for reporting
    getStats() {
        return {
            successRate: this.successRate.toFixed(2),
            concurrency: this.concurrency,
            batchSize: this.batchSize,
            avgResponseTime: `${this.getAverageResponseTime().toFixed(0)}ms`
        };
    }
};

// --- Global Variables & State ---
let successCount = 0; // Counter for successful emails sent
let failureCount = 0; // Counter for failed emails (after retries)
let successfulEmails = []; // Temporary list to hold successful emails before saving to file
let failedEmails = []; // Temporary list to hold failed emails before saving to file
const dnsFailureQueue = []; // Queue for emails that failed with 451 DNS error (if SKIP_DNS_FAILURES is true)
const failedSmtpEmails = new Map(); // Tracks SMTP servers that have failed (key: email, value: { retryTime, backoffLevel, reason })
const transporterCache = new Map(); // Caches Nodemailer transporter instances for reuse (key: smtpId)
const invalidDomainCache = new Set(); // Caches domains confirmed to have no valid MX records
let emailsProcessedSinceLastPause = 0; // Counter for the periodic pause feature

// --- Progress Bar Setup ---
const progressBar = new cliProgress.SingleBar({
    format: `Sending | ${chalk.cyan('{bar}')} | {percentage}% | {value}/{total} | ETA: {eta}s | Speed: {speed} email/s`,
    hideCursor: true,
    clearOnComplete: false, // Keep the bar visible upon completion
    stopOnComplete: true,
    noTTYOutput: false, // Ensure output even if not a TTY (useful for some CI/runners)
    stream: process.stdout, // Output to standard out
    barsize: 40,
    etaBuffer: 100, // Samples for ETA calculation
    formatValue: (v, options, type) => {
        // Custom formatter to add speed
        if (type === 'total' || type === 'value') {
            return cliProgress.Format.ValueFormat(v, options, type);
        }
        if (type === 'speed') {
            return `${v} email/s`;
        }
        return cliProgress.Format.ValueFormat(v, options, type);
    }
}, cliProgress.Presets.shades_classic);
let progressBarActive = false; // Track if the progress bar is currently running
let totalEmails = 0; // Total emails to be processed, used for progress bar total


// --- Logger Setup (Winston) ---
const logFormat = winston.format.printf(({ timestamp, level, message }) => {
    // Conditionally hide MX record errors from console if QUIET_MX_ERRORS is true
    if (level === 'warn' && message.includes('MX Record Error') && process.env.QUIET_MX_ERRORS === 'true') {
         // Log to file but not console
         return null; // Returning null prevents logging by this specific transport format
    }
    return `${timestamp} ${level}: ${message}`;
});

const logger = winston.createLogger({
    level: process.env.LOG_LEVEL || 'info', // Default log level
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        logFormat // Use the custom format
    ),
    transports: [
        // Console Transport - respects QUIET_MX_ERRORS via logFormat
        new winston.transports.Console({
            level: process.env.CONSOLE_LOG_LEVEL || 'info', // Separate level for console
            format: winston.format.combine(
                winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
                 // Add colorization to console output
                 winston.format.colorize(),
                 // Apply the filtering format AFTER colorization
                 logFormat
            )
        }),
        // File Transports - always log everything regardless of QUIET_MX_ERRORS
        new winston.transports.File({
            filename: 'error.log',
            level: 'error',
            format: winston.format.combine( // Ensure files also get timestamp etc.
                winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
                winston.format.json() // Log errors as JSON for easier parsing
            )
        }),
        new winston.transports.File({
             filename: 'combined.log',
             format: winston.format.combine(
                 winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
                 winston.format.json() // Log combined as JSON
             )
         })
    ]
});

// Helper function to safely log messages without messing up the progress bar
const safeLog = (level, message, ...meta) => {
    if (progressBarActive) {
        progressBar.stop();
        logger[level](message, ...meta);
        progressBar.start(totalEmails, successCount + failureCount); // Resume with current progress
        // Force redraw if needed, though start usually handles it
        // if (progressBar.lastDrawnString) progressBar.update(progressBar.value);
    } else {
        logger[level](message, ...meta);
    }
};

// Override console methods to use the safe logger
const originalConsoleLog = console.log;
console.log = function(...args) {
    // Format arguments into a single string message
    const message = args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : arg).join(' ');
     // Use 'info' level for console.log, avoid direct logging if possible
     // If it's chalked output, log directly to avoid winston color codes clashing
    if (typeof args[0] === 'string' && args[0].includes('\u001b[')) { // Check for ANSI escape codes (chalk)
        if (progressBarActive) progressBar.stop();
        originalConsoleLog.apply(console, args);
        if (progressBarActive) progressBar.start(totalEmails, successCount + failureCount);
    } else {
       safeLog('info', message);
    }
};

const originalConsoleError = console.error;
console.error = function(...args) {
    const message = args.map(arg => arg instanceof Error ? arg.stack : (typeof arg === 'object' ? JSON.stringify(arg) : arg)).join(' ');
    // Use 'error' level for console.error
    if (typeof args[0] === 'string' && args[0].includes('\u001b[')) { // Check for ANSI escape codes (chalk)
        if (progressBarActive) progressBar.stop();
        originalConsoleError.apply(console, args);
        if (progressBarActive) progressBar.start(totalEmails, successCount + failureCount);
    } else {
      safeLog('error', message);
    }
};

// Centralized error handler
const handleError = (error, context = '') => {
    const message = `Error ${context ? `in ${context}: ` : ''}${error.message}`;
    safeLog('error', message);
    if (error.stack) safeLog('error', error.stack);
};


// --- Network & DNS Utilities ---

// Checks basic network connectivity to a host using DNS lookup
const checkServerConnection = async (host) => {
    try {
        await dnsLookup(host); // Simple lookup to see if the host resolves
        return true;
    } catch (error) {
        // Log as warning, as it might be a temporary DNS issue not specific to the host
        safeLog('warn', `DNS lookup failed for ${host}: ${error.message}. Server might be offline or DNS issues exist.`);
        return false;
    }
};

// Validates if a domain has MX records (with timeout)
const validateDomain = async (domain) => {
    // Option to disable all MX validation globally
    if (process.env.DISABLE_ALL_MX_VALIDATION === 'true') {
        return true;
    }
    // Check cache first
    if (invalidDomainCache.has(domain)) {
        return false;
    }
    // Option to skip domain validation (treats all as valid)
    if (process.env.SKIP_DOMAIN_VALIDATION === 'true') {
        return true;
    }

    try {
        // Race DNS resolution against a timeout to prevent hangs
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new Error(`DNS MX lookup timeout for ${domain}`)), 3000); // 3 second timeout
        });

        const mxRecords = await Promise.race([
            resolveMx(domain),
            timeoutPromise
        ]);

        // Check if any MX records were returned
        if (mxRecords && mxRecords.length > 0) {
            return true;
        } else {
            // No MX records found, considered invalid
            safeLog('warn', `MX Record Error: No valid MX records found for domain: ${domain}`, { domain });
            invalidDomainCache.add(domain); // Cache the invalid domain
            return false;
        }
    } catch (error) {
         // Handle specific DNS errors
        if (error.code === 'ENODATA' || error.code === 'ENOTFOUND') {
             safeLog('warn', `MX Record Error: Domain ${domain} not found or no data (Code: ${error.code})`, { domain, errorCode: error.code });
        } else if (error.message.includes('timeout')) {
            safeLog('warn', `MX Record Error: DNS lookup timed out for domain: ${domain}`, { domain });
             // Do not cache timeouts, as they might be temporary
        } else {
             // Log other unexpected errors
             safeLog('error', `MX Record Error: Unexpected error validating domain ${domain}: ${error.message}`, { domain, errorCode: error.code });
        }
        invalidDomainCache.add(domain); // Cache as invalid on error (except maybe timeout? debatable)
        return false;
    }
};


// --- SMTP Management ---

// Gets or creates a Nodemailer transporter instance for a given SMTP config
const getTransporter = async (smtpConfig) => {
    const smtpId = `${smtpConfig.email}:${smtpConfig.host}:${smtpConfig.port}`; // Unique ID for caching

    // Return cached transporter if available
    if (transporterCache.has(smtpId)) {
        return transporterCache.get(smtpId);
    }

    // Check if the SMTP server is known to be failing
    if (failedSmtpEmails.has(smtpConfig.email)) {
         const failureInfo = failedSmtpEmails.get(smtpConfig.email);
         const now = Date.now();
         if (now < failureInfo.retryTime) {
             // safeLog('warn', `SMTP ${smtpConfig.email} is temporarily marked as failed. Skipping. Reason: ${failureInfo.reason}`);
             return null; // Still marked as failed
         } else {
             // Retry time has passed, remove from failed list and allow creation attempt
             safeLog('info', `SMTP ${smtpConfig.email} retry time reached. Attempting to use again.`);
             failedSmtpEmails.delete(smtpConfig.email);
         }
    }


    // Optional: Check basic server connectivity before creating transporter (can add delay)
    // if (process.env.CHECK_SMTP_CONNECTIVITY === 'true') {
    //     const serverOnline = await checkServerConnection(smtpConfig.host);
    //     if (!serverOnline) {
    //         safeLog('error', `SMTP host ${smtpConfig.host} appears unreachable.`);
    //         // Mark as failed temporarily
    //         failedSmtpEmails.set(smtpConfig.email, {
    //             retryTime: Date.now() + 300000, // 5 minutes
    //             backoffLevel: 1,
    //             reason: 'Host unreachable'
    //         });
    //         return null;
    //     }
    // }

    // Create new transporter instance with optimized settings
    const transporter = nodemailer.createTransport({
        host: smtpConfig.host,
        port: smtpConfig.port || 587, // Default to 587 if port not specified
        secure: smtpConfig.secure || false, // Use TLS (true for 465, false for 587/25)
        auth: {
            user: smtpConfig.email,
            pass: smtpConfig.password
        },
        pool: true, // Use connection pooling
        maxConnections: adaptiveControl.concurrency > 3 ? 5 : 3, // Limit pool size per transporter instance (adjust based on overall concurrency)
        maxMessages: 100, // Messages per connection before renewal
        rateLimit: false, // Disable internal rate limiting, rely on our RateLimiter
        connectionTimeout: 15000, // 15 seconds
        socketTimeout: 20000, // 20 seconds
        greetingTimeout: 10000, // 10 seconds
        debug: process.env.NODE_ENV === 'development', // Enable debug logs only in development
        logger: process.env.NODE_ENV === 'development' ? console : false, // Use console for logs in dev
        disableFileAccess: false, // Allow file access for attachments
        disableUrlAccess: true, // Disable URL access for security/speed
        tls: {
            rejectUnauthorized: process.env.SMTP_REJECT_UNAUTHORIZED !== 'false' // Allow self-signed certs if needed
        }
    });

    // Optionally verify SMTP connection (can be slow, disabled by default)
    if (!SMTP_TEST && !SKIP_SMTP_VERIFY) {
        try {
            await transporter.verify();
            safeLog('info', `✅ SMTP ${smtpConfig.email} verified successfully.`);
        } catch (error) {
            safeLog('error', `❌ SMTP ${smtpConfig.email} verification failed: ${error.message}`);
            // Decide whether to still try using it (e.g., for timeouts) or mark as failed
            if (error.code === 'ETIMEDOUT' || error.code === 'ESOCKET' || error.message.includes('Timeout')) {
                safeLog('warn', `⚠️ Verification timed out for SMTP ${smtpConfig.email}, will attempt to use anyway.`);
                // Cache it even if verification failed on timeout, might work for sending
                 transporterCache.set(smtpId, transporter);
                 return transporter;
            } else {
                // Mark as failed for non-timeout verification errors (e.g., auth)
                failedSmtpEmails.set(smtpConfig.email, {
                    retryTime: Date.now() + 600000, // 10 minutes for verification failure
                    backoffLevel: 1,
                    reason: `Verification failed: ${error.message}`
                });
                return null; // Verification failed, don't cache or use
            }
        }
    } else if (!SMTP_TEST && SKIP_SMTP_VERIFY) {
         // If skipping verification, log it once per SMTP for clarity
         // safeLog('info', `ℹ️ Skipping SMTP verification for ${smtpConfig.email}.`);
    }

    // Cache the successfully created or timeout-skipped transporter
    transporterCache.set(smtpId, transporter);
    return transporter;
};

// Loads SMTP server configurations from a file
const loadSmtpServers = async (filePath) => {
    safeLog('info', `Loading SMTP configurations from: ${filePath}`);
    try {
        const data = await fs.promises.readFile(filePath, 'utf-8');
        const lines = data.split('\n').map(line => line.trim()).filter(line => line && !line.startsWith('#')); // Ignore empty lines and comments
        const configs = lines.map(line => {
            const parts = line.split('|');
            if (parts.length < 3) {
                safeLog('warn', `Skipping invalid SMTP line (expected host|email|password[|port|secure]): ${line}`);
                return null;
            }
            const [host, email, password, portStr, secureStr] = parts;
            // Basic validation
             if (!host || !email || !password || !EmailValidator.validate(email)) {
                 safeLog('warn', `Skipping invalid SMTP config: host, email, or password missing/invalid in line: ${line}`);
                 return null;
             }
            return {
                host: host.trim(),
                email: email.trim(),
                password: password.trim(),
                port: portStr ? parseInt(portStr.trim(), 10) : 587, // Default port 587
                secure: secureStr ? secureStr.trim().toLowerCase() === 'true' : false // Default secure false
            };
        }).filter(config => config !== null); // Filter out invalid lines

        safeLog('info', `Loaded ${configs.length} valid SMTP configurations.`);
        if (configs.length === 0) {
             safeLog('error', 'No valid SMTP configurations found in the file. Cannot proceed.');
             process.exit(1);
         }
        return configs;
    } catch (error) {
        handleError(error, 'loading SMTP servers');
        safeLog('error', 'Failed to load SMTP configurations. Exiting.');
        process.exit(1); // Exit if SMTPs can't be loaded
        return []; // Should not be reached due to process.exit
    }
};


// --- Recipient Loading ---

// Reads recipient emails from a file, validates format, optionally checks MX, handles duplicates
async function* loadRecipientsInBatches(filePath, batchSize) {
    safeLog('info', `Loading recipients from: ${filePath}`);
    const checkMx = process.env.CHECK_MX_DURING_LOAD === 'true' && process.env.DISABLE_ALL_MX_VALIDATION !== 'true';
    safeLog('info', `MX check during load: ${checkMx ? 'Enabled' : 'Disabled'}`);

    const fileStream = createReadStream(filePath, { encoding: 'utf-8' });
    const rl = createInterface({ input: fileStream, crlfDelay: Infinity }); // Handles different line endings

    let batch = []; // Current batch being built
    const seenEmails = new Set(); // Track emails to avoid duplicates
    let lineCount = 0;
    let validCount = 0;
    let invalidFormatCount = 0;
    let duplicateCount = 0;
    let mxFailureCount = 0;

    for await (const line of rl) {
        lineCount++;
        const email = line.trim();

        if (!email) continue; // Skip empty lines

        // 1. Validate Email Format
        if (!EmailValidator.validate(email)) {
            // safeLog('warn', `Invalid email format skipped: ${email} (Line: ${lineCount})`);
            invalidFormatCount++;
            continue;
        }

        // 2. Check for Duplicates
        if (seenEmails.has(email)) {
             // safeLog('warn', `Duplicate email skipped: ${email} (Line: ${lineCount})`);
            duplicateCount++;
            continue;
        }
        seenEmails.add(email); // Add to seen set


        // 3. Optional: Validate Domain MX Record
        if (checkMx) {
            const domain = email.split('@')[1];
            try {
                const isValidDomain = await validateDomain(domain); // Use the cached validation function
                if (!isValidDomain) {
                     // safeLog('warn', `Skipping email due to invalid/missing MX record for domain ${domain}: ${email} (Line: ${lineCount})`);
                    mxFailureCount++;
                    // Optionally add to failed list immediately? No, let send attempt handle final failure logging.
                    continue; // Skip this email
                }
                // Domain is valid (or validation skipped/disabled), add to batch
                 batch.push(email);
                 validCount++;

            } catch (error) {
                 // Log error during MX check but still include the email - better to try sending than lose it.
                 safeLog('error', `Error checking MX for ${domain} (email: ${email}, Line: ${lineCount}): ${error.message}. Including email anyway.`);
                 batch.push(email); // Include email despite MX check error
                 validCount++;
            }

        } else {
             // MX check disabled, just add the email
             batch.push(email);
             validCount++;
        }


        // 4. Yield Batch When Full
        if (batch.length >= batchSize) {
            yield batch; // Return the completed batch
            batch = []; // Reset for the next batch
        }
    }

    // Yield any remaining emails in the last batch
    if (batch.length > 0) {
        yield batch;
    }

     safeLog('info', `Recipient loading complete. Lines read: ${lineCount}, Valid & Unique: ${validCount}, Invalid Format: ${invalidFormatCount}, Duplicates: ${duplicateCount}, MX Failures (during load): ${mxFailureCount}`);
     totalEmails = validCount; // Set the total count for the progress bar
}


// --- Email Content & Personalization ---

// Generates a random alphanumeric string of a given length
const generateRandomString = (length = 8) => {
    return Math.random().toString(36).substring(2, 2 + length);
};

// Fills placeholders in the email template
const fillTemplate = (template, recipientEmail) => {
    // Extract a potential name from the email address (simple approach)
    const namePart = recipientEmail.split('@')[0];
    const name = namePart
        .replace(/[\._\-\d]+/g, ' ') // Replace dots, underscores, hyphens, digits with space
        .replace(/\s+/g, ' ') // Collapse multiple spaces
        .trim()
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()) // Capitalize each word
        .join(' ');

    // Replace placeholders
    return template
        .replace(/{name}/g, name || 'Cliente') // Use extracted name or fallback
        .replace(/{email}/g, recipientEmail)
        .replace(/{date}/g, new Date().toLocaleDateString('pt-BR')) // Brazilian date format
        .replace(/{time}/g, new Date().toLocaleTimeString('pt-BR')) // Brazilian time format
        // Replace {random} or {random:length} (e.g., {random:10})
        .replace(/{random(?::(\d+))?}/g, (_, lenStr) => {
            const length = lenStr ? parseInt(lenStr, 10) : 9; // Default length 9 if not specified
            return generateRandomString(length);
        });
};


// --- SMTP Server Selection Logic ---

// Selects the next available SMTP server, handling failures and retries
const getNextSmtpConfig = (smtpServers) => {
    const now = Date.now();
    let availableServers = [];
    let unavailableCount = 0;

    // Check failed SMTPs and see if any can be retried
    for (const config of smtpServers) {
         if (failedSmtpEmails.has(config.email)) {
             const failureInfo = failedSmtpEmails.get(config.email);
             if (now < failureInfo.retryTime) {
                 unavailableCount++;
                 // Still failed, skip this one
                 continue;
             } else {
                 // Retry time elapsed, make it available again
                 safeLog('info', `SMTP ${config.email} is now available for retry (was failing due to: ${failureInfo.reason}).`);
                 failedSmtpEmails.delete(config.email); // Remove from failed map
                 availableServers.push(config); // Add to available list
             }
         } else {
             // Not in the failed map, it's available
             availableServers.push(config);
         }
     }


    // Log status if some servers are unavailable
    if (unavailableCount > 0) {
         safeLog('warn', `${unavailableCount}/${smtpServers.length} SMTP server(s) are currently marked as unavailable due to previous errors.`);
         // Could add more detail here if needed, listing reasons and retry times
     }


    // Handle case where no servers are available
    if (availableServers.length === 0) {
        safeLog('error', 'All SMTP servers are currently marked as unavailable. Cannot select an SMTP server.');
        // Check if there are *any* servers defined at all
         if (smtpServers.length === 0) {
             throw new Error('No SMTP servers loaded.');
         }
         // If servers exist but are all failed, check when the next one *might* be available
         let nextRetryTime = Infinity;
         failedSmtpEmails.forEach(info => {
             if (info.retryTime < nextRetryTime) nextRetryTime = info.retryTime;
         });
         const waitMinutes = nextRetryTime === Infinity ? 'N/A' : Math.ceil((nextRetryTime - now) / 60000);
         throw new Error(`All ${smtpServers.length} SMTP servers are currently unavailable. Next potential retry in ~${waitMinutes} minutes.`);
    }

    // Select a random server from the available ones for better load distribution
    const randomIndex = Math.floor(Math.random() * availableServers.length);
    return availableServers[randomIndex];
};


// --- Rate Limiter ---
// Controls the number of concurrent asynchronous operations (like sending emails)
class RateLimiter {
    constructor(concurrency = 1) {
        this.queue = []; // Tasks waiting to run
        this.active = 0; // Number of tasks currently running
        this._concurrency = concurrency; // Maximum active tasks allowed
        safeLog('info', `RateLimiter initialized with concurrency: ${concurrency}`);
    }

    // Getter for concurrency
    get concurrency() {
        return this._concurrency;
    }

    // Setter for concurrency (allows dynamic adjustment)
    set concurrency(newConcurrency) {
        const oldConcurrency = this._concurrency;
        this._concurrency = Math.max(1, newConcurrency); // Ensure concurrency is at least 1
        if (oldConcurrency !== this._concurrency) {
           safeLog('info', `RateLimiter concurrency updated to: ${this._concurrency}`);
        }
        // If concurrency increased, potentially start more tasks from the queue
        this.next();
    }


    // Executes a function `fn` respecting the concurrency limit
    async run(fn) {
        return new Promise((resolve, reject) => {
            // Wrapper function to handle execution and queue management
             const task = async () => {
                 this.active++;
                 try {
                     const result = await fn();
                     resolve(result); // Resolve the promise when fn completes
                 } catch (error) {
                     reject(error); // Reject the promise if fn throws an error
                 } finally {
                     this.active--; // Decrement active count
                     this.next(); // Try to run the next task in the queue
                 }
             };

            if (this.active < this.concurrency) {
                // If below limit, run immediately
                 task();
            } else {
                // If limit reached, add to queue
                this.queue.push(task);
            }
        });
    }

    // Checks the queue and runs the next task if possible
    next() {
        while (this.queue.length > 0 && this.active < this.concurrency) {
            const nextTask = this.queue.shift(); // Get the oldest task
            if (nextTask) {
                nextTask(); // Execute it
            }
        }
    }
}

// Initialize the global rate limiter using the initial concurrency from adaptive control
const rateLimiter = new RateLimiter(adaptiveControl.concurrency);

// --- Send Progress Display ---
// Logs the status of individual email sends without disrupting the progress bar
const showSendProgress = (type, recipient, smtp, details = '') => {
     // Temporarily stop the progress bar to print the message clearly
     if (progressBarActive) progressBar.stop();

     let message = '';
     switch (type) {
         case 'sending':
             // Optional: Could show a subtle sending message if desired
             // message = chalk.gray(`⏳ Sending to ${recipient} via ${smtp}...`);
             break; // Often better to omit 'sending' to reduce noise, show only final status
         case 'success':
             message = chalk.green(`✅ SUCCESS: ${recipient} via ${smtp}`);
             if (details) message += ` ${chalk.greenBright(details)}`;
             break;
         case 'retry':
             message = chalk.blue(`🔄 RETRY: ${recipient} via ${smtp}`);
             if (details) message += ` - ${chalk.blueBright(details)}`;
             break;
         case 'error':
             message = chalk.red(`❌ FAILURE: ${recipient} via ${smtp}`);
             if (details) message += ` - ${chalk.redBright(details)}`;
             break;
          case 'queued':
             message = chalk.yellow(`🕒 QUEUED: ${recipient} via ${smtp}`);
             if (details) message += ` - ${chalk.yellowBright(details)}`;
             break;
     }

     if (message) {
          // Use originalConsoleLog to bypass winston/safeLog formatting for direct console feedback
          originalConsoleLog(message);
      }

     // Resume the progress bar
      if (progressBarActive) progressBar.start(totalEmails, successCount + failureCount);
 };


// --- Performance Metrics Tracking ---
const metrics = {
    startTime: Date.now(),
    totalSentAttempts: 0, // Includes retries
    totalSuccess: 0,
    totalFailure: 0, // Final failures after retries
    smtpUsage: new Map(), // Counts successful sends per SMTP email
    domainStats: new Map(), // Tracks success/failure per recipient domain
    sendTimes: [], // Window of recent successful send times (ms)
    lastReportTime: Date.now(),

    recordAttempt() {
        this.totalSentAttempts++;
    },

    recordSuccess(smtpEmail, domain, sendTimeMs) {
        this.totalSuccess++;
        this.smtpUsage.set(smtpEmail, (this.smtpUsage.get(smtpEmail) || 0) + 1);

        // Update domain stats
        const stats = this.domainStats.get(domain) || { success: 0, failure: 0 };
        stats.success++;
        this.domainStats.set(domain, stats);

        // Add send time to window (keep last 100)
        this.sendTimes.push(sendTimeMs);
        if (this.sendTimes.length > 100) this.sendTimes.shift();
    },

    recordFailure(domain, errorType = 'Unknown') {
        this.totalFailure++;

        // Update domain stats
        const stats = this.domainStats.get(domain) || { success: 0, failure: 0 };
        stats.failure++;
        this.domainStats.set(domain, stats);
        // Note: We don't record failure time here, only success times for average send duration
    },

    getAverageSendTime() {
        if (this.sendTimes.length === 0) return 'N/A';
        const avg = this.sendTimes.reduce((sum, time) => sum + time, 0) / this.sendTimes.length;
        return `${avg.toFixed(0)}ms`;
    },

    generateReport() {
        const elapsedSeconds = (Date.now() - this.startTime) / 1000;
        const emailsPerSecond = elapsedSeconds > 0 ? (this.totalSuccess / elapsedSeconds).toFixed(2) : '0.00';

        const topSmtps = [...this.smtpUsage.entries()]
            .sort((a, b) => b[1] - a[1]) // Sort descending by count
            .slice(0, 5); // Take top 5

        const problemDomains = [...this.domainStats.entries()]
            .filter(([_, stats]) => stats.failure > 3 && stats.failure > stats.success) // Domains with >3 failures and more failures than successes
            .sort((a, b) => b[1].failure - a[1].failure) // Sort by failure count
            .slice(0, 5); // Take top 5

        return {
            totalSuccess: this.totalSuccess,
            totalFailure: this.totalFailure,
            elapsedTime: `${elapsedSeconds.toFixed(0)}s`,
            emailsPerSecond,
            avgSendTime: this.getAverageSendTime(),
            topSmtps,
            problemDomains,
            adaptiveStats: adaptiveControl.getStats() // Include adaptive control stats
        };
    },

    logPeriodicReport(force = false) {
        const now = Date.now();
        // Report every 60 seconds or if forced
        if (force || now - this.lastReportTime > 60000) {
            const report = this.generateReport();
            if (progressBarActive) progressBar.stop(); // Pause progress bar

            originalConsoleLog(chalk.yellowBright('\n--- Performance Report ---'));
            originalConsoleLog(chalk.white(`  Elapsed Time: ${report.elapsedTime}`));
            originalConsoleLog(chalk.green(`  Successful Sends: ${report.totalSuccess}`));
            originalConsoleLog(chalk.red(`  Failed Sends: ${report.totalFailure}`));
            originalConsoleLog(chalk.blue(`  Avg. Speed: ${report.emailsPerSecond} emails/s`));
            originalConsoleLog(chalk.blue(`  Avg. Send Time: ${report.avgSendTime}`));
            originalConsoleLog(chalk.magenta(`  Adaptive Control: Concurrency=${report.adaptiveStats.concurrency}, BatchSize=${report.adaptiveStats.batchSize}, SuccessRate=${(report.adaptiveStats.successRate * 100).toFixed(1)}%`));


            if (report.topSmtps.length > 0) {
                originalConsoleLog(chalk.cyan('  Top SMTP Servers (by success count):'));
                report.topSmtps.forEach(([smtp, count], index) => {
                    originalConsoleLog(`    ${index + 1}. ${smtp}: ${count} sends`);
                });
            }

            if (report.problemDomains.length > 0) {
                 originalConsoleLog(chalk.redBright('  Domains with High Failure Rate (>3 failures & failure > success):'));
                 report.problemDomains.forEach(([domain, stats], index) => {
                     originalConsoleLog(`    ${index + 1}. ${domain}: ${stats.failure} failures / ${stats.success} successes`);
                 });
            }
            originalConsoleLog(chalk.yellowBright('--------------------------\n'));

            this.lastReportTime = now;
             if (progressBarActive) progressBar.start(totalEmails, successCount + failureCount); // Resume progress bar
        }
    }
};

// --- Core Email Sending Function ---
const sendEmail = async (smtpConfig, recipient, subject, html, attachmentPath, retries = 0, initialSmtpEmail, smtpServersRef) => {
    const startTime = Date.now();
    metrics.recordAttempt(); // Record the attempt (initial or retry)
    const domain = recipient.split('@')[1] || 'unknown_domain'; // Extract domain for metrics

    try {
        // Basic domain format check (redundant if done in loading, but safe)
        if (domain === 'unknown_domain' || domain.indexOf('.') === -1) {
             throw new Error('Invalid recipient email domain format');
         }

        // Optional: MX Check just before sending (if not done during load and not disabled)
        if (process.env.CHECK_MX_BEFORE_SEND === 'true' &&
            process.env.CHECK_MX_DURING_LOAD !== 'true' &&
            process.env.DISABLE_ALL_MX_VALIDATION !== 'true') {
            if (!await validateDomain(domain)) {
                throw new Error('MX Record Check Failed: Domain likely invalid or unreachable');
            }
        }

        // Get transporter (might return null if SMTP is marked failed)
        const transporter = await getTransporter(smtpConfig);
        if (!transporter) {
             // This SMTP is currently unavailable (marked failed)
             throw new Error(`SMTP server ${smtpConfig.email} is currently marked as unavailable.`);
        }

        // Show 'sending' status only on the first attempt for a recipient
        // if (retries === 0) {
        //     showSendProgress('sending', recipient, smtpConfig.email);
        // }

        const mailOptions = {
            from: `"${process.env.FROM_NAME || 'Claro Br'}" <${smtpConfig.email}>`, // Use FROM_NAME from .env or fallback
            to: recipient,
            subject: subject,
            html: html,
            attachments: attachmentPath ? [{ path: attachmentPath }] : [], // Add attachment only if path is provided
            priority: 'high',
            headers: { // Standard headers for high priority
                'X-Priority': '1 (Highest)',
                'X-MSMail-Priority': 'High',
                'Importance': 'High'
            },
            disableFileAccess: false, // Ensure attachments work
            disableUrlAccess: true, // Speed/security
            // Some providers might recommend disabling their own MX lookups if you've done it
            // dnsTimeout: 5000 // Example: Shorter DNS timeout within Nodemailer
        };

        // --- SMTP Test Mode ---
        if (SMTP_TEST) {
            // Simulate successful send in test mode
            const simulatedDelay = Math.random() * 100 + 50; // 50-150ms delay
            await new Promise(resolve => setTimeout(resolve, simulatedDelay));
            safeLog('info', `[TEST MODE] Simulated send to ${recipient} via ${smtpConfig.email}`);
            showSendProgress('success', recipient, smtpConfig.email, '[TEST MODE]');
            metrics.recordSuccess(smtpConfig.email, domain, simulatedDelay);
            adaptiveControl.registerResult(true, simulatedDelay);
            return true; // Indicate success
        }

        // --- Actual Send ---
        const info = await transporter.sendMail(mailOptions);
        const sendTime = Date.now() - startTime;

        // Log success
        showSendProgress('success', recipient, smtpConfig.email, `(${sendTime}ms)`);
        metrics.recordSuccess(smtpConfig.email, domain, sendTime);
        adaptiveControl.registerResult(true, sendTime);

        // Check for potential issues even on success (e.g., deferred messages - less common)
        // if (info.pending?.length > 0) {
        //     safeLog('warn', `Email to ${recipient} was deferred by the server: ${info.response}`);
        // }

         // Increment counter for periodic pause feature
         emailsProcessedSinceLastPause++;

        return true; // Indicate success

    } catch (error) {
        const sendTime = Date.now() - startTime;
        adaptiveControl.registerResult(false, sendTime); // Register failure with adaptive control

        // --- Sophisticated Error Handling & Retry Logic ---
        const errorCode = error.code || 'UNKNOWN';
        const responseCode = error.responseCode || 0;
        const errorMessage = error.message.toLowerCase();

        // Define categories of errors
        const isAuthError = errorCode === 'EAUTH' || responseCode === 535 || errorMessage.includes('authentication failed') || errorMessage.includes('invalid login') || errorMessage.includes('invalid credentials');
        const isRecipientError = responseCode === 550 || responseCode === 553 || errorMessage.includes('recipient rejected') || errorMessage.includes('user unknown') || errorMessage.includes('no such user');
        const isMxError = errorMessage.includes('mx record check failed') || errorMessage.includes("can't find a valid mx") || errorCode === 'ENODATA' || errorCode === 'ENOTFOUND' || responseCode === 550 && errorMessage.includes('domain'); // 550 can be recipient or domain
        const isDnsTimeoutError = errorMessage.includes('dns lookup timeout'); // Custom timeout from validateDomain
        const isConnectionError = ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'ESOCKET', 'EHOSTUNREACH'].includes(errorCode) || errorMessage.includes('timeout');
        const isTemporaryError = responseCode === 421 || responseCode === 450 || responseCode === 451 || responseCode === 452 || errorMessage.includes('try again later') || errorMessage.includes('temporary failure') || errorMessage.includes('resource temporarily unavailable');
        const isDns451Error = responseCode === 451 && (errorMessage.includes('dns') || errorMessage.includes('4.4.0')); // Specific DNS temporary error


        // 1. Handle Authentication Errors
        if (isAuthError) {
             safeLog('error', `Authentication failed for SMTP ${smtpConfig.email}: ${error.message}. Marking as failed.`);
             showSendProgress('error', recipient, smtpConfig.email, 'Auth Failure');
             // Mark this SMTP as failed (long duration for auth errors)
             failedSmtpEmails.set(smtpConfig.email, {
                 retryTime: Date.now() + 1800000, // 30 minutes
                 backoffLevel: 3, // Higher backoff level
                 reason: `Authentication Failed: ${error.message}`
             });
             // Remove from active transporter cache
              const smtpId = `${smtpConfig.email}:${smtpConfig.host}:${smtpConfig.port}`;
              transporterCache.delete(smtpId);

             // Do NOT retry with the same SMTP. Attempt retry with a *different* SMTP if this was the first try.
             if (retries === 0) {
                 safeLog('warn', `Attempting send to ${recipient} with a different SMTP server due to auth failure on ${smtpConfig.email}.`);
                 try {
                     const nextSmtpConfig = getNextSmtpConfig(smtpServersRef); // Get a different available SMTP
                     // Retry immediately with the new SMTP, resetting retry count for the new server
                     return sendEmail(nextSmtpConfig, recipient, subject, html, attachmentPath, 0, nextSmtpConfig.email, smtpServersRef);
                 } catch (nextSmtpError) {
                      // If no other SMTPs are available
                      safeLog('error', `No alternative SMTP servers available after auth failure on ${smtpConfig.email} for ${recipient}. Failing permanently.`);
                      metrics.recordFailure(domain, `Auth failure on ${smtpConfig.email}, no alternative SMTP`);
                      failedEmails.push({ email: recipient, error: `Auth failure on ${smtpConfig.email}, no alternative SMTP` });
                      return false;
                 }
             } else {
                  // If this was already a retry, fail permanently for this recipient
                  safeLog('error', `Authentication failure on retry for ${recipient} via ${smtpConfig.email}. Failing permanently.`);
                  metrics.recordFailure(domain, `Auth failure on retry: ${smtpConfig.email}`);
                  failedEmails.push({ email: recipient, error: `Auth failure on retry with ${smtpConfig.email}: ${error.message}` });
                  return false;
             }
        }

        // 2. Handle Permanent Recipient/MX Errors (No Retry)
        if (isRecipientError || isMxError || isDnsTimeoutError) {
            const reason = isRecipientError ? 'Recipient Rejected' : (isMxError ? 'MX/Domain Error' : 'DNS Timeout');
            safeLog('warn', `Permanent failure for ${recipient}: ${reason}. ${error.message}`);
            showSendProgress('error', recipient, smtpConfig.email, reason);
            metrics.recordFailure(domain, `${reason}: ${error.message}`);
            failedEmails.push({ email: recipient, error: `${reason}: ${error.message}` }); // Add specific error
            return false; // No retry for these errors
        }


       // 3. Handle Specific DNS 451 Error (Queue or Retry with Backoff)
       if (isDns451Error) {
           // Option 1: Queue for later processing if enabled and first attempt
           if (SKIP_DNS_FAILURES && retries === 0) {
               safeLog('warn', `Queueing ${recipient} for later retry due to 451 DNS error from ${smtpConfig.email}.`);
               showSendProgress('queued', recipient, smtpConfig.email, '451 DNS Error - Queued');
               dnsFailureQueue.push({ recipient, subject, html, attachmentPath, attemptedAt: Date.now(), initialSmtp: smtpConfig.email });
               // Don't record as final failure yet, just return false for this attempt
               return false;
           }
           // Option 2: Retry with longer backoff if not queuing or already retrying
           const maxDnsRetries = MAX_RETRIES + 3; // Allow more retries for DNS issues
           if (retries < maxDnsRetries) {
                const delay = Math.min(RETRY_DELAY * (2.5 ** retries), 60000); // Exponential backoff, max 60s
                safeLog('warn', `Temporary DNS error 451 for ${recipient} via ${smtpConfig.email}. Retrying in ${delay}ms (Attempt ${retries + 1}/${maxDnsRetries})...`);
                showSendProgress('retry', recipient, smtpConfig.email, `451 DNS Error - Wait ${Math.round(delay/1000)}s`);
                await new Promise(resolve => setTimeout(resolve, delay));
                // Try with a *different* SMTP server for the retry
                try {
                     const nextSmtpConfig = getNextSmtpConfig(smtpServersRef);
                     return sendEmail(nextSmtpConfig, recipient, subject, html, attachmentPath, retries + 1, initialSmtpEmail, smtpServersRef);
                 } catch (nextSmtpError) {
                     safeLog('error', `No alternative SMTP available for DNS 451 retry for ${recipient}. Failing permanently.`);
                     metrics.recordFailure(domain, `DNS 451 error, no alternative SMTP for retry`);
                     failedEmails.push({ email: recipient, error: `DNS 451 error, no alternative SMTP for retry: ${error.message}` });
                     return false;
                 }

           } else {
                safeLog('error', `Permanent failure for ${recipient} after ${maxDnsRetries} attempts for 451 DNS error.`);
                showSendProgress('error', recipient, smtpConfig.email, `451 DNS Error - Max Retries`);
                metrics.recordFailure(domain, `451 DNS Error - Max Retries Exceeded`);
                failedEmails.push({ email: recipient, error: `451 DNS Error - Max Retries Exceeded: ${error.message}` });
                return false;
           }
       }


        // 4. Handle General Temporary/Connection Errors (Retry with Backoff)
        if (isTemporaryError || isConnectionError) {
            if (retries < MAX_RETRIES) {
                const delay = Math.min(RETRY_DELAY * (2 ** retries), 30000); // Exponential backoff, max 30s
                const reason = isConnectionError ? 'Connection Error' : 'Temporary Server Error';
                safeLog('warn', `${reason} for ${recipient} via ${smtpConfig.email}. Retrying in ${delay}ms (Attempt ${retries + 1}/${MAX_RETRIES})... Error: ${error.message}`);
                showSendProgress('retry', recipient, smtpConfig.email, `${reason} - Wait ${Math.round(delay/1000)}s`);
                await new Promise(resolve => setTimeout(resolve, delay));
                 // Try with a *different* SMTP server for the retry
                 try {
                     const nextSmtpConfig = getNextSmtpConfig(smtpServersRef);
                     return sendEmail(nextSmtpConfig, recipient, subject, html, attachmentPath, retries + 1, initialSmtpEmail, smtpServersRef);
                 } catch (nextSmtpError) {
                     safeLog('error', `No alternative SMTP available for ${reason} retry for ${recipient}. Failing permanently.`);
                     metrics.recordFailure(domain, `${reason}, no alternative SMTP for retry`);
                     failedEmails.push({ email: recipient, error: `${reason}, no alternative SMTP for retry: ${error.message}` });
                     return false;
                 }
            } else {
                // Max retries reached for temporary/connection errors
                 const reason = isConnectionError ? 'Connection Error' : 'Temporary Server Error';
                 safeLog('error', `Permanent failure for ${recipient} after ${MAX_RETRIES} attempts for ${reason}. Error: ${error.message}`);
                 showSendProgress('error', recipient, smtpConfig.email, `${reason} - Max Retries`);
                 metrics.recordFailure(domain, `${reason} - Max Retries Exceeded`);
                 failedEmails.push({ email: recipient, error: `${reason} - Max Retries Exceeded: ${error.message}` });
                 return false;
            }
        }


        // 5. Handle Other/Unknown Errors (Retry cautiously or Fail)
        safeLog('error', `Unhandled error for ${recipient} via ${smtpConfig.email}. Code: ${errorCode}, ResponseCode: ${responseCode}, Message: ${error.message}`);
        if (retries < MAX_RETRIES) {
             // Apply a standard retry for unknown errors
             const delay = Math.min(RETRY_DELAY * (2 ** retries), 30000);
             safeLog('warn', `Unknown error type. Retrying in ${delay}ms (Attempt ${retries + 1}/${MAX_RETRIES})...`);
             showSendProgress('retry', recipient, smtpConfig.email, `Unknown Error - Wait ${Math.round(delay/1000)}s`);
             await new Promise(resolve => setTimeout(resolve, delay));
              // Try with a *different* SMTP server
             try {
                  const nextSmtpConfig = getNextSmtpConfig(smtpServersRef);
                  return sendEmail(nextSmtpConfig, recipient, subject, html, attachmentPath, retries + 1, initialSmtpEmail, smtpServersRef);
              } catch (nextSmtpError) {
                  safeLog('error', `No alternative SMTP available for Unknown Error retry for ${recipient}. Failing permanently.`);
                  metrics.recordFailure(domain, `Unknown error, no alternative SMTP for retry`);
                  failedEmails.push({ email: recipient, error: `Unknown error, no alternative SMTP for retry: ${error.message}` });
                  return false;
              }
        } else {
             // Max retries reached for unknown errors
             safeLog('error', `Permanent failure for ${recipient} after ${MAX_RETRIES} attempts for Unknown Error. Error: ${error.message}`);
             showSendProgress('error', recipient, smtpConfig.email, 'Unknown Error - Max Retries');
             metrics.recordFailure(domain, `Unknown Error - Max Retries Exceeded`);
             failedEmails.push({ email: recipient, error: `Unknown Error - Max Retries Exceeded: ${error.message}` });
             return false;
        }
    }
};


// --- Batch Processing ---
const processBatch = async (batch, smtpServers, subjectTemplate, htmlTemplate, attachmentPath) => {
    // Use the adaptive batch size for logging, though actual processing is concurrent via rate limiter
    const adaptiveBatchSize = adaptiveControl.getBatchSize();

    // Stop bar for logging batch info
    if (progressBarActive) progressBar.stop();
    originalConsoleLog(chalk.cyan(`\n--- Processing Batch of ${batch.length} (Adaptive Target: ${adaptiveBatchSize}) ---`));
    const adaptiveStats = adaptiveControl.getStats();
    originalConsoleLog(chalk.magenta(`  Adaptive Stats: Concurrency=${adaptiveStats.concurrency}, SuccessRate=${(adaptiveStats.successRate*100).toFixed(1)}%, AvgResponse=${adaptiveStats.avgResponseTime}`));
    // Resume bar
    if (progressBarActive) progressBar.start(totalEmails, successCount + failureCount);


    const batchPromises = batch.map(recipient => {
        // Run each email send through the rate limiter
        return rateLimiter.run(async () => {
            let success = false;
            try {
                // Select an SMTP server for this specific email
                // Pass the reference to the full list for retry mechanism
                const smtpConfig = getNextSmtpConfig(smtpServers);
                 // Personalize subject and body for each recipient
                const html = fillTemplate(htmlTemplate, recipient);
                const subject = fillTemplate(subjectTemplate, recipient); // Allow personalization in subject too

                // Attempt to send the email (includes internal retries)
                success = await sendEmail(smtpConfig, recipient, subject, html, attachmentPath, 0, smtpConfig.email, smtpServers);

            } catch (error) {
                 // Handle errors *getting* an SMTP or other unexpected issues before sendEmail is called
                 safeLog('error', `Critical error processing ${recipient} before send: ${error.message}`);
                 metrics.recordFailure(recipient.split('@')[1] || 'unknown', `Preprocessing error: ${error.message}`);
                 failedEmails.push({ email: recipient, error: `Preprocessing error: ${error.message}` });
                 success = false; // Ensure it's marked as failure
            }

            // Update global counters based on the final result of sendEmail (including retries)
             if (success) {
                 successCount++;
                 successfulEmails.push(recipient); // Add to temporary success list
             } else {
                 failureCount++;
                 // Failed emails are added to failedEmails list within sendEmail on final failure
             }
             // Update progress bar after each email is fully processed (success or final failure)
              if (progressBarActive) progressBar.increment(1, { speed: metrics.generateReport().emailsPerSecond });

               // --- Periodic Pause Logic ---
               // Pause briefly every N emails to potentially ease load or mimic human behavior
               const pauseInterval = parseInt(process.env.PAUSE_INTERVAL_EMAILS, 10) || 0; // e.g., 50
               const pauseDuration = parseInt(process.env.PAUSE_DURATION_MS, 10) || 0; // e.g., 5000 (5s)

               if (pauseInterval > 0 && pauseDuration > 0 && emailsProcessedSinceLastPause >= pauseInterval) {
                   if (progressBarActive) progressBar.stop();
                   originalConsoleLog(chalk.yellow(`\n⏸️ Pausing for ${pauseDuration / 1000} seconds after ${emailsProcessedSinceLastPause} emails...\n`));
                   await new Promise(resolve => setTimeout(resolve, pauseDuration));
                   emailsProcessedSinceLastPause = 0; // Reset counter
                   if (progressBarActive) progressBar.start(totalEmails, successCount + failureCount);
                   originalConsoleLog(chalk.green('▶️ Resuming email sending...'));
               }


              // Log periodic performance report
              metrics.logPeriodicReport();

              return success; // Return status for Promise.all
         });
     });

     // Wait for all promises in the current conceptual batch to complete
     await Promise.all(batchPromises);
      // Note: We don't log batch summary here as individual results are logged and progress bar updates
};


// --- Reporting ---

// Appends email addresses to a CSV file
const saveReport = async (filePath, emailsToSave) => {
    if (!emailsToSave || emailsToSave.length === 0) {
        return; // Nothing to save
    }
    safeLog('info', `Saving ${emailsToSave.length} emails to ${filePath}...`);

     // Determine if the file exists to decide whether to write headers
     const fileExists = fs.existsSync(filePath);

    // Create a write stream in append mode
    const ws = fs.createWriteStream(filePath, { flags: 'a', encoding: 'utf-8' });
    const csvStream = fastcsv.format({
         headers: !fileExists, // Write headers only if file doesn't exist
         writeHeaders: !fileExists, // Explicitly control header writing
         quoteColumns: true // Quote fields to handle potential commas etc.
      });

    csvStream.pipe(ws);

     // Write each email object or string
     emailsToSave.forEach(item => {
         if (typeof item === 'string') {
             csvStream.write({ email: item, error: '' }); // Assume success if just string
         } else if (typeof item === 'object' && item.email) {
             csvStream.write({ email: item.email, error: item.error || '' }); // Include error if present
         }
     });

    csvStream.end();

    return new Promise((resolve, reject) => {
        ws.on('finish', () => {
            safeLog('info', `Successfully saved report to ${filePath}`);
            resolve();
        });
        ws.on('error', (error) => {
            safeLog('error', `Error saving report to ${filePath}: ${error.message}`);
            reject(error);
        });
    });
};


// --- Main Execution Function ---
async function main() {
    metrics.startTime = Date.now(); // Record start time accurately
    console.log(chalk.cyan(`
    ╔═══════════════════════════════════════════╗
    ║          NodeMailer Bulk Sender           ║
    ║          Initialization Started           ║
    ╚═══════════════════════════════════════════╝
    `));

    // --- Configuration ---
    const config = {
        smtpFile: process.env.SMTP_CONFIG_PATH,
        recipientFile: process.env.RECIPIENT_LIST_PATH,
        successFile: process.env.SUCCESS_FILE_PATH || path.join(process.cwd(), 'sucesso.csv'), // Default to current dir
        failedFile: process.env.FAILED_FILE_PATH || path.join(process.cwd(), 'falha.csv'),     // Default to current dir
        attachmentPath: process.env.ATTACHMENT_PATH,
        templatePath: process.env.MESSAGE_FILE_PATH,
        subjectTemplate: process.env.EMAIL_SUBJECT || '📩 18% OFF Pagando HOJE: Evite Bloqueio. Cod:{random:8}', // Use SUBJECT from env or default
        // Parse optional periodic pause settings
        pauseInterval: parseInt(process.env.PAUSE_INTERVAL_EMAILS, 10) || 0,
        pauseDuration: parseInt(process.env.PAUSE_DURATION_MS, 10) || 0,
    };

    // Ensure output directories exist (create if necessary)
    const successDir = path.dirname(config.successFile);
    const failedDir = path.dirname(config.failedFile);
    if (!fs.existsSync(successDir)) fs.mkdirSync(successDir, { recursive: true });
    if (!fs.existsSync(failedDir)) fs.mkdirSync(failedDir, { recursive: true });


    // --- Load Data ---
    const smtpServers = await loadSmtpServers(config.smtpFile);
    const htmlTemplate = await fs.promises.readFile(config.templatePath, 'utf-8');


    // --- Load Recipients and Initialize Progress Bar ---
    console.log(chalk.blue('⏳ Loading recipients... (This may take time if MX check is enabled)'));
    const recipientGenerator = loadRecipientsInBatches(config.recipientFile, 1000); // Use larger batch for loading efficiency
    // We need the total count first for the progress bar
    // This requires iterating through the generator once to count, then again to process.
    // Alternatively, load all into memory if feasible, or estimate total.
    // Let's load into memory for simplicity IF the file isn't excessively large.
    // CAUTION: This can consume significant memory for huge lists.
    const allRecipients = [];
     for await (const batch of recipientGenerator) {
         allRecipients.push(...batch);
     }
     totalEmails = allRecipients.length; // Set total for progress bar accurately

     if (totalEmails === 0) {
         console.log(chalk.yellow('⚠️ No valid recipients found after filtering. Exiting.'));
         return;
     }

     console.log(chalk.green(`✅ Loaded ${totalEmails} valid and unique recipients.`));


    // Determine MX validation mode for logging
    let mxValidationMode = 'Disabled';
    if (process.env.DISABLE_ALL_MX_VALIDATION !== 'true') {
        if (process.env.CHECK_MX_DURING_LOAD === 'true') mxValidationMode = 'During Load';
        else if (process.env.CHECK_MX_BEFORE_SEND === 'true') mxValidationMode = 'Before Send';
    }

    // --- Log Initial Setup ---
    console.log(chalk.cyan(`
    --- Configuration Summary ---
    SMTP Servers: ${smtpServers.length}
    Total Recipients: ${totalEmails}
    Subject: "${config.subjectTemplate}"
    Attachment: ${config.attachmentPath ? path.basename(config.attachmentPath) : 'None'}
    Template File: ${path.basename(config.templatePath)}
    Initial Concurrency: ${adaptiveControl.concurrency} (Adaptive: [${adaptiveControl.minConcurrency}-${adaptiveControl.maxConcurrency}])
    Conceptual Batch Size: ${adaptiveControl.batchSize} (Adaptive: [${adaptiveControl.minBatchSize}-${adaptiveControl.maxBatchSize}])
    Max Retries: ${MAX_RETRIES}
    Retry Delay: ${RETRY_DELAY}ms (Exponential)
    SMTP Verification: ${SKIP_SMTP_VERIFY ? 'Skipped' : 'Enabled'}
    MX Validation Mode: ${mxValidationMode}
    Queue 451 DNS Errors: ${SKIP_DNS_FAILURES ? 'Yes' : 'No'}
    SMTP Test Mode: ${SMTP_TEST ? chalk.yellow('YES') : 'No'}
    Periodic Pause: ${config.pauseInterval > 0 ? `Every ${config.pauseInterval} emails for ${config.pauseDuration}ms` : 'Disabled'}
    Success Log: ${config.successFile}
    Failure Log: ${config.failedFile}
    ---------------------------
    `));

    console.log(chalk.blue('🚀 Starting email sending process...'));
    progressBar.start(totalEmails, 0, { speed: "N/A" }); // Start progress bar
    progressBarActive = true;

    // --- Process Recipients in Adaptive Batches ---
    let processedCount = 0;
    while (processedCount < totalEmails) {
        const currentBatchSize = adaptiveControl.getBatchSize(); // Get current adaptive batch size
        const batch = allRecipients.slice(processedCount, processedCount + currentBatchSize);
        processedCount += batch.length;

        if (batch.length === 0) break; // Should not happen, but safety check

         // Process the batch using the rate limiter internally
        await processBatch(batch, smtpServers, config.subjectTemplate, htmlTemplate, config.attachmentPath);

        // Save reports periodically after each batch to avoid large memory build-up
        await saveReport(config.successFile, successfulEmails);
        successfulEmails = []; // Clear temporary list
        await saveReport(config.failedFile, failedEmails);
        failedEmails = []; // Clear temporary list

        // Optional: Short delay between conceptual batches (already handled by rate limiter mostly)
        // if (processedCount < totalEmails && DELAY_BETWEEN_BATCHES > 0) {
        //    await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
        //}
    }

    // --- Process DNS Failure Queue ---
    if (dnsFailureQueue.length > 0) {
        progressBar.stop(); // Stop main progress bar
        progressBarActive = false;
        console.log(chalk.yellow(`\n--- Retrying ${dnsFailureQueue.length} emails from DNS 451 Failure Queue ---`));

        // Initialize a new progress bar for the queue
        const queueProgressBar = new cliProgress.SingleBar({
            format: `DNS Queue | ${chalk.yellow('{bar}')} | {percentage}% | {value}/{total} | ETA: {eta}s`,
            hideCursor: true,
            clearOnComplete: false,
            stopOnComplete: true,
        }, cliProgress.Presets.shades_classic);
        queueProgressBar.start(dnsFailureQueue.length, 0);


        // Sort queue by original attempt time (oldest first)
        dnsFailureQueue.sort((a, b) => a.attemptedAt - b.attemptedAt);

         // Use a slower rate for retries from the queue
         const queueRateLimiter = new RateLimiter(Math.max(1, Math.floor(adaptiveControl.minConcurrency / 2))); // Slower concurrency

         const queuePromises = dnsFailureQueue.map(item => {
             return queueRateLimiter.run(async () => {
                 let success = false;
                 try {
                     // Add a longer initial delay before trying queued items
                     await new Promise(resolve => setTimeout(resolve, 3000)); // 3-second delay

                     safeLog('info', `Retrying queued email to ${item.recipient} (Original SMTP: ${item.initialSmtp})`);
                     const smtpConfig = getNextSmtpConfig(smtpServers); // Get a fresh available SMTP
                     success = await sendEmail(
                         smtpConfig,
                         item.recipient,
                         item.subject, // Use original personalized subject/html
                         item.html,
                         item.attachmentPath,
                         0, // Reset retry count for this attempt from queue
                         smtpConfig.email,
                         smtpServers
                     );
                 } catch (error) {
                     safeLog('error', `Error retrying queued email ${item.recipient}: ${error.message}`);
                     success = false;
                     failedEmails.push({ email: item.recipient, error: `Failed on queue retry: ${error.message}` });
                 }

                 if (success) {
                     successCount++;
                     successfulEmails.push(item.recipient);
                 } else {
                     failureCount++;
                     // Failure reason added within sendEmail or catch block above
                 }
                 queueProgressBar.increment(); // Update queue progress bar
                 return success;
             });
         });

        await Promise.all(queuePromises);
        queueProgressBar.stop();

         // Save final results from the queue
         await saveReport(config.successFile, successfulEmails);
         successfulEmails = [];
         await saveReport(config.failedFile, failedEmails);
         failedEmails = [];
    }


    // --- Final Summary ---
    progressBar.stop(); // Ensure main progress bar is stopped
    progressBarActive = false;
    metrics.logPeriodicReport(true); // Log final report

    const endTime = Date.now();
    const totalDurationSeconds = Math.floor((endTime - metrics.startTime) / 1000);
    const finalAvgSpeed = totalDurationSeconds > 0 ? (successCount / totalDurationSeconds).toFixed(2) : 'N/A';

    console.log(chalk.greenBright(`
    ╔═══════════════════════════════════════════╗
    ║              Process Complete             ║
    ╚═══════════════════════════════════════════╝
    `));
    console.log(`  Total Time: ${totalDurationSeconds} seconds`);
    console.log(chalk.green(`  Successful Sends: ${successCount} (${((successCount / totalEmails) * 100).toFixed(1)}%)`));
    console.log(chalk.red(`  Failed Sends: ${failureCount}`));
    console.log(`  Final Average Speed: ${finalAvgSpeed} emails/s`);
    console.log(`  Success logs saved to: ${config.successFile}`);
    console.log(`  Failure logs saved to: ${config.failedFile}`);
    console.log(chalk.cyan('---------------------------------------------'));
}

// --- Graceful Shutdown & Signal Handling ---
let isShuttingDown = false;
async function gracefulShutdown() {
    if (isShuttingDown) return; // Prevent multiple shutdowns
    isShuttingDown = true;
    console.log(chalk.yellow('\n🛑 SIGINT received. Starting graceful shutdown...'));
    if (progressBarActive) {
        progressBar.stop();
        progressBarActive = false;
    }

    // 1. Stop accepting new tasks (RateLimiter doesn't have a direct stop, rely on loop exit)
    //    Main loop should naturally terminate if process exit is called.

    // 2. Wait briefly for active tasks (optional, depends on how termination is handled)
    //    await new Promise(resolve => setTimeout(resolve, 1000)); // e.g., wait 1 second

    // 3. Save any remaining data in buffers
    console.log(chalk.yellow('💾 Saving pending reports...'));
    try {
        await saveReport(process.env.SUCCESS_FILE_PATH || './sucesso.csv', successfulEmails);
        await saveReport(process.env.FAILED_FILE_PATH || './falha.csv', failedEmails);
         // Also save DNS queue items as failed if interrupted? Or maybe a separate 'interrupted' file?
         if(dnsFailureQueue.length > 0){
              const interruptedFile = path.join(path.dirname(process.env.FAILED_FILE_PATH || './falha.csv'), 'interrupted_dns_queue.csv');
               console.log(chalk.yellow(`💾 Saving ${dnsFailureQueue.length} pending DNS queue emails to ${interruptedFile}...`));
               await saveReport(interruptedFile, dnsFailureQueue.map(item => ({ email: item.recipient, error: 'Interrupted before DNS queue processing' })));
         }

        console.log(chalk.green('✅ Pending reports saved.'));
    } catch (error) {
        console.error(chalk.red('❌ Error saving reports during shutdown:'), error);
    }

    // 4. Close resources (loggers, etc.) - Winston usually handles this on exit
    winston.loggers.close();

    console.log(chalk.yellow('👋 Exiting now.'));
    process.exit(0); // Exit cleanly
}

// Handle SIGINT (Ctrl+C)
process.on('SIGINT', gracefulShutdown);
// Handle SIGTERM (sent by process managers like PM2)
process.on('SIGTERM', gracefulShutdown);

// Optional: Handle SIGUSR1 to reset failed SMTP list during runtime
process.on('SIGUSR1', () => {
     if (progressBarActive) progressBar.stop();
     console.log(chalk.yellow('\n🔄 SIGUSR1 received. Resetting failed SMTP server list...'));
     const count = failedSmtpEmails.size;
     failedSmtpEmails.clear(); // Clear the map
     transporterCache.clear(); // Clear transporter cache as well, forcing re-creation/re-verification
     console.log(chalk.green(`✅ Cleared ${count} failed SMTP entries. They will be retried on next use.`));
     if (progressBarActive) progressBar.start(totalEmails, successCount + failureCount);
 });


// --- Start the application ---
main().catch(error => {
    // Catch unhandled errors in the main async function
    if (progressBarActive) progressBar.stop();
    console.error(chalk.red('\n💥 Unhandled critical error in main execution:'), error);
    process.exit(1); // Exit with error code
});