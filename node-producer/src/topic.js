export function createTopics(kafkaClient) {
    const { TOPIC, PARTITIONS, REPLICATION_FACTOR } = process.env;

    kafkaClient.createTopics([
        {
            topic: TOPIC,
            partitions: Number(PARTITIONS) ?? 1,
            replicationFactor: Number(REPLICATION_FACTOR) ?? 2
        }
    ], (_, message) => {
        if (message) {
            console.error('Erreur lors de la création du topic : ', message);
        } else {
            console.log('Topic créé avec succès !');
        }
    });
}
