const express = require("express");
const bodyParser = require("body-parser");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");
const amqp = require("amqplib");
const mysql = require("mysql");
const fs = require("fs");

require("dotenv").config();
const app = express();
const PORT = 3000;
const exchange = "events";

app.use(bodyParser.json());

// MySQL connection setup
const db = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
});

// RabbitMQ connection setup
const RABBITMQ_URL = process.env.RABBITMQ_URL;
let channel;

async function createChannel() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);

    channel = await connection.createChannel();
  } catch (error) {
    console.error("Error connecting to RabbitMQ: ", error);
  }
}

// Function to publish event to RabbitMQ
function publishEvent(event) {
  // Ensure the exchange exists
  channel.assertExchange(exchange, "fanout", { durable: false });

  // Convert the event to a JSON string
  const eventString = JSON.stringify(event);

  // Publish the event to the exchange
  channel.publish(exchange, "", Buffer.from(eventString));
}

db.connect((err) => {
  if (err) {
    console.error("Error connecting to MySQL: " + err.stack);
    return;
  }
  console.log("Connected to MySQL as id " + db.threadId);
});

// Registration endpoint
app.post("/register", async (req, res) => {
  const { username, email, password } = req.body;

  // Hash the password
  const hashedPassword = await bcrypt.hash(password, 10);

  // Insert user data into MySQL
  // Use appropriate error handling
  const insertQuery = `INSERT INTO user (username, email, password) VALUES (?, ?, ?)`;
  db.query(insertQuery, [username, email, hashedPassword], (err, result) => {
    if (err) {
      console.error("Error registering user: " + err.stack);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      // Publish event to RabbitMQ
      const event = {
        eventType: "userRegistered",
        userId: result.insertId,
        username,
        email,
      };

      publishEvent(event);

      res.json({ message: "User registered successfully" });
    }
  });
});

// Authentication endpoint
app.post("/login", async (req, res) => {
  const { email, password } = req.body;

  // Retrieve user from MySQL based on email
  const selectQuery = `SELECT * FROM user WHERE email = ?`;
  db.query(selectQuery, [email], async (err, results) => {
    if (err) {
      console.error("Error querying user: " + err.stack);
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      if (results.length === 0) {
        res.status(401).json({ error: "Invalid credentials" });
      } else {
        const user = results[0];

        // Compare the provided password with the hashed password in the database
        const isPasswordValid = await bcrypt.compare(password, user.password);

        if (isPasswordValid) {
          // Generate JWT
          const token = jwt.sign(
            { userId: user.id, username: user.username },
            "your-secret-key",
            { expiresIn: "1h" }
          );

          res.json({ token });
        } else {
          res.status(401).json({ error: "Invalid credentials" });
        }
      }
    }
  });
});

const mongoose = require("mongoose");
// Connect to MongoDB
mongoose.connect(process.env.MONGO_URL);

// Define MongoDB schema and model
const gameDataSchema = new mongoose.Schema({
  userId: { type: Number, required: true },
  points: { type: Number, require: true },
});

const GameData = mongoose.model("GameData", gameDataSchema);

// Create new game entry
app.post("/game", async (req, res) => {
  const { userId, points } = req.body;

  // Create new game entry in MongoDB
  const gameData = new GameData({ userId, points });
  gameData
    .save()
    .then(() => {
      res.json({ message: "Game data created successfully" });
    })
    .catch((error) => {
      console.error("Error creating game data: " + error.message);
      res.status(500).json({ error: "Internal Server Error" });
    });
});

// Retrieve game data for a specific user
app.get("/game/:userId", async (req, res) => {
  const userId = req.params.userId;

  // Retrieve game data for a specific user from MongoDB
  GameData.find({ userId })
    .then((gameData) => {
      res.json(gameData);
    })
    .catch((error) => {
      console.error("Error retrieving game data: " + error.message);
      res.status(500).json({ error: "Internal Server Error" });
    });
});

// Update game data for a specific user
app.put("/game/:userId", async (req, res) => {
  const userId = req.params.userId;
  const updatedGameData = req.body;

  // Update game data for a specific user in MongoDB
  GameData.updateOne({ userId }, updatedGameData)
    .then(() => {
      res.json({ message: "Game data updated successfully" });
    })
    .catch((error) => {
      console.error("Error updating game data: " + error.message);
      res.status(500).json({ error: "Internal Server Error" });
    });
});

// Delete a game entry
app.delete("/game/:userId", async (req, res) => {
  const userId = req.params.userId;

  // Delete game data for a specific user from MongoDB
  GameData.deleteOne({ userId })
    .then(() => {
      res.json({ message: "Game data deleted successfully" });
    })
    .catch((error) => {
      console.error("Error deleting game data: " + error.message);
      res.status(500).json({ error: "Internal Server Error" });
    });
});

async function subscribeToEvents() {
  await createChannel();
  // Ensure the exchange exists
  await channel.assertExchange(exchange, "fanout", { durable: false });

  // Create a temporary queue for this subscriber
  const { queue } = await channel.assertQueue("", { exclusive: true });

  // Bind the queue to the exchange
  await channel.bindQueue(queue, exchange, "");

  console.log(`Waiting for events. To exit, press CTRL+C`);

  // Consume messages from the queue
  channel.consume(
    queue,
    (msg) => {
      if (msg.content) {
        const event = JSON.parse(msg.content.toString());
        // Log the event to a file (you can customize this part based on your needs)
        logEventToFile(event);
      }
    },
    { noAck: true }
  );
}

// Function to log the event to a file
function logEventToFile(event) {
  const logFilePath = "events.log";

  const logEntry = `${new Date().toISOString()} - Event: ${JSON.stringify(
    event
  )}\n`;

  fs.appendFile(logFilePath, logEntry, (err) => {
    if (err) {
      console.error("Error writing to log file: " + err.message);
    } else {
      console.log("Event logged successfully");
    }
  });
}

// Start the event subscriber
subscribeToEvents().catch((error) => {
  console.error("Error setting up event subscriber: ", error);
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
