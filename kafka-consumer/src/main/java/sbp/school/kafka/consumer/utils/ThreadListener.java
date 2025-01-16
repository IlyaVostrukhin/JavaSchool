package sbp.school.kafka.consumer.utils;

import sbp.school.kafka.consumer.service.ConsumerService;

public class ThreadListener extends Thread {

    private final ConsumerService consumerService;

    public ThreadListener(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    private void listen() {
        consumerService.listen();
    }

    @Override
    public void run() {
        listen();
    }
}
