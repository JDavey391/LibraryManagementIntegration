const mysql = require('mysql2/promise');
require('dotenv').config();

// Create a connection pool to MySQL
const db = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Helper function to initialize database tables
async function initializeDatabase() {
    try {
        console.log("Connecting to MySQL Database...");
        
        // 1. Create Books Table
        await db.query(`
            CREATE TABLE IF NOT EXISTS books (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                author VARCHAR(255) NOT NULL,
                available_copies INT DEFAULT 1,
                total_copies INT DEFAULT 1
            )
        `);

        // 2. Create Loans Table
        await db.query(`
            CREATE TABLE IF NOT EXISTS loans (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                book_id INT NOT NULL,
                borrow_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                due_date TIMESTAMP NULL,
                status VARCHAR(50) DEFAULT 'BORROWED',
                FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
            )
        `);

        // Seed initial mock books if the table is empty
        const [rows] = await db.query('SELECT COUNT(*) as count FROM books');
        if (rows[0].count === 0) {
            await db.query(`
                INSERT INTO books (title, author, available_copies, total_copies) VALUES
                ('The Great Gatsby', 'F. Scott Fitzgerald', 3, 3),
                ('To Kill a Mockingbird', 'Harper Lee', 2, 2),
                ('1984', 'George Orwell', 5, 5)
            `);
            console.log("🌱 Database seeded with initial books!");
        }

        console.log("✅ MySQL Database tables initialized successfully!");
    } catch (error) {
        console.error("❌ Database initialization failed:", error.message);
    }
}

module.exports = { db, initializeDatabase };