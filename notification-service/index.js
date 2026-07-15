const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
require('dotenv').config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3002;

// Connect to MongoDB using Mongoose
mongoose.connect(process.env.MONGO_URI)
    .then(() => console.log("✅ Successfully connected to MongoDB!"))
    .catch(err => console.error("❌ MongoDB connection error:", err));

// Define Schema
const NotificationSchema = new mongoose.Schema({
    loanId: Number,
    userId: String,
    bookTitle: String,
    dueDate: Date,
    status: { type: String, default: 'SENT' },
    sentAt: { type: Date, default: Date.now }
});

const Notification = mongoose.model('Notification', NotificationSchema);

async function consumeQueue() {
    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URL);
        const channel = await connection.createChannel();
        
        await channel.assertQueue("library.loans", { durable: true });
        console.log("📥 Waiting for loan events in RabbitMQ queue 'library.loans'...");

        channel.consume("library.loans", async (msg) => {
            if (msg !== null) {
                const rawContent = msg.content.toString();
                
                // 1. Guard against corrupted payload structures in the queue
                if (rawContent === "[object Object]") {
                    console.warn("⚠️ Discarding invalid raw object structure from queue.");
                    channel.ack(msg);
                    return;
                }

                let messageContent;
                try {
                    messageContent = JSON.parse(rawContent);
                } catch (parseError) {
                    console.error("⚠️ Failed to parse message body. Discarding invalid JSON:", parseError.message);
                    channel.ack(msg); 
                    return;
                }

                console.log("\n📦 New Loan Event Received:", messageContent);

                try {
                    // 2. Simulate sending the notification alert
                    console.log(`✉️ Sending notification to User [${messageContent.userId}]...`);
                    console.log(`📢 "Hi ${messageContent.userId}, you successfully checked out '${messageContent.bookTitle}'. Due date: ${new Date(messageContent.dueDate).toLocaleDateString()}"`);

                    // 3. Log historical notification event to MongoDB
                    const log = new Notification({
                        loanId: messageContent.loanId,
                        userId: messageContent.userId,
                        bookTitle: messageContent.bookTitle,
                        dueDate: messageContent.dueDate
                    });
                    await log.save();
                    console.log("💾 Notification event successfully logged to MongoDB!");

                    channel.ack(msg);
                } catch (error) {
                    console.error("❌ Error processing notification database write:", error);
                    channel.nack(msg); 
                }
            }
        });

    } catch (error) {
        console.warn("⚠️ RabbitMQ is not ready yet. Retrying in 5 seconds...");
        setTimeout(consumeQueue, 5000);
    }
}

app.get('/api/v1/notifications', async (req, res) => {
    try {
        const logs = await Notification.find().sort({ sentAt: -1 });
        res.json(logs);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Notification Service actively running on http://localhost:${PORT}`);
    consumeQueue();
});