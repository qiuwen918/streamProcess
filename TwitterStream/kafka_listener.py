from kafka import SimpleProducer, KafkaClient
from tweepy import OAuthHandler, Stream, StreamListener

consumer_key="TJvzCy2cqUfikudrKLYxbbNP5"
consumer_secret="tKFrZkHIwTpCexqV2IjYEERHu9OymeYcbOZXa8OAxVK925nIv3"

access_token="1180247386574077953-6uCtKMtMD5Smn5ZoVohFd42nvUha8h"
access_token_secret="ZRNlCwQFGTwnPqCXj2qYd4gCNUsRSZtPFAbNYYtuuqVnm"

class KafkaListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        producer.send_messages("twitter-stream", data.encode("utf-8"))
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    kafka_client = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka_client)

    l = KafkaListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['#'])

