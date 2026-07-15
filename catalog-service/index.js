const express = require('express');
const jwt = require('jsonwebtoken');
const amqp = require('amqplib');
const { db, initializeDatabase } = require('./db');
require('dotenv').config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;
let channel = null; // We will store our RabbitMQ channel connection here

// Connect to RabbitMQ Message Queue
async function connectQueue() {
    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URL);
        channel = await connection.createChannel();
        // Assert a durable queue so messages survive broker restarts
        await channel.assertQueue("library.loans", { durable: true });
        console.log("✅ Successfully connected to RabbitMQ broker!");
    } catch (error) {
        console.warn("⚠️ RabbitMQ is not ready yet. Retrying in 5 seconds...");
        setTimeout(connectQueue, 5000);
    }
}

// Simple JWT Verification Middleware (Protects write/delete routes)
function authenticateToken(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) return res.status(401).json({ error: "Access token missing. Please log in first." });

    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ error: "Invalid or expired token." });
        req.user = user;
        next();
    });
}

// --- API ENDPOINTS ---

// 1. Mock Login (Generates JWT for testing)
app.post('/api/v1/auth/login', (req, res) => {
    const { username } = req.body;
    if (!username) return res.status(400).json({ error: "Username is required to log in." });
    
    // Create a simple payload and token
    const token = jwt.sign({ user: username }, process.env.JWT_SECRET, { expiresIn: '1h' });
    res.json({ token });
});

// 2. GET /api/v1/books - Public Catalog Listing
app.get('/api/v1/books', async (req, res) => {
    try {
        const [books] = await db.query('SELECT * FROM books');
        res.json(books);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 3. POST /api/v1/borrow - Secured (Asynchronous checkout flow)
app.post('/api/v1/borrow', authenticateToken, async (req, res) => {
    const { userId, bookId } = req.body;
    if (!userId || !bookId) {
        return res.status(400).json({ error: "Missing required fields: userId or bookId." });
    }

    try {
        // Find if book exists and has copies available
        const [books] = await db.query('SELECT * FROM books WHERE id = ?', [bookId]);
        if (books.length === 0) return res.status(404).json({ error: "Book not found." });

        const book = books[0];
        if (book.available_copies <= 0) {
            return res.status(400).json({ error: "No physical copies of this book are currently available." });
        }

        // Deduct copy inventory
        await db.query('UPDATE books SET available_copies = available_copies - 1 WHERE id = ?', [bookId]);

        // Create loan entry (Due date: 14 days from now)
        const dueDate = new Date();
        dueDate.setDate(dueDate.getDate() + 14);
        
        const [result] = await db.query(
            'INSERT INTO loans (user_id, book_id, due_date) VALUES (?, ?, ?)',
            [userId, bookId, dueDate]
        );

        // Prepare message payload for RabbitMQ
        const messagePayload = {
            event: "LOAN_CREATED",
            loanId: result.insertId,
            userId,
            bookTitle: book.title,
            dueDate: dueDate.toISOString()
        };

        // Dispatch asynchronously to the queue
        if (channel) {
            channel.sendToQueue(
                "library.loans",
                Buffer.from(JSON.stringify(messagePayload)),
                { persistent: true }
            );
            console.log("📨 Message dispatched to queue: LOAN_CREATED", messagePayload.loanId);
        } else {
            console.warn("⚠️ Queue connection unavailable. Message was not queued.");
        }

        res.status(201).json({
            message: "Loan processed successfully!",
            loanId: result.insertId,
            dueDate
        });

    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 4. PUT /api/v1/books/:id - Secured (Update inventory)
app.put('/api/v1/books/:id', authenticateToken, async (req, res) => {
    const { title, author, available_copies, total_copies } = req.body;
    try {
        await db.query(
            'UPDATE books SET title = ?, author = ?, available_copies = ?, total_copies = ? WHERE id = ?',
            [title, author, available_copies, total_copies, req.params.id]
        );
        res.json({ message: "Book specifications updated successfully." });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// 5. DELETE /api/v1/books/:id - Secured (Remove inventory)
app.delete('/api/v1/books/:id', authenticateToken, async (req, res) => {
    try {
        const [result] = await db.query('DELETE FROM books WHERE id = ?', [req.params.id]);
        if (result.affectedRows === 0) return res.status(404).json({ error: "Book not found." });
        res.json({ message: "Book purged from system catalog." });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Startup Server
async function startServer() {
    await initializeDatabase();
    await connectQueue();
    app.listen(PORT, () => {
        console.log(`🚀 Catalog Service actively running on http://localhost:${PORT}`);
    });
}

startServer();