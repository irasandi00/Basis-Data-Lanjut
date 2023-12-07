package org.example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public class JobQueue {

    private final Jedis jedis;
    private final String queueKey;

    public JobQueue(String host, int port, String queueName) {
        this.jedis = new Jedis(host, port);
        this.queueKey = queueName;
    }

    public void pushJob(String job) {
        try {
            jedis.rpush(queueKey, job);
        } catch (JedisDataException e) {
            System.out.println("Error pushing job to queue: " + e.getMessage());
        }
    }

    public String popJob() {
        try {
            return jedis.lpop(queueKey);
        } catch (JedisDataException e) {
            System.out.println("Error popping job from queue: " + e.getMessage());
            return null;
        }
    }

    public long getQueueLength() {
        try {
            return jedis.llen(queueKey);
        } catch (JedisDataException e) {
            System.out.println("Error getting queue length: " + e.getMessage());
            return -1;
        }
    }

    public void closeConnection() {
        jedis.close();
    }

    public static void main(String[] args) {
        // Ganti nilai host, port, dan nama queue sesuai konfigurasi Redis Anda
        String redisHost = "localhost";
        int redisPort = 6379;
        String queueName = "myQueue";

        JobQueue jobQueue = new JobQueue(redisHost, redisPort, queueName);

        // Push beberapa pekerjaan ke dalam antrian
        jobQueue.pushJob("Job 1");
        jobQueue.pushJob("Job 2");
        jobQueue.pushJob("Job 3");

        // Ambil pekerjaan dari antrian (FIFO)
        String job = jobQueue.popJob();
        if (job != null) {
            System.out.println("Processed job: " + job);
        } else {
            System.out.println("No job available.");
        }

        // Mendapatkan panjang antrian
        long queueLength = jobQueue.getQueueLength();
        System.out.println("Queue length: " + queueLength);

        // Menutup koneksi Redis
        jobQueue.closeConnection();
    }
}
