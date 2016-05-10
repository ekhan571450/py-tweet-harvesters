## Configuration

To set up the application environment on Ubunutu 14.04 LTS:

    sh configure

## Streaming API program

Use all command line arguments to launch the program:

    python3 streaming.py --couchip IP_GOES_HERE --consumerkey CONSUMER_KEY --consumersecret CONSUMER_SECRET --tokenkey TOKEN_KEY --tokensecret TOKEN_SECRET

Or as a background process (although it would be better to use a process manager):

    nohup python3 streaming.py --couchip IP_GOES_HERE --consumerkey CONSUMER_KEY --consumersecret CONSUMER_SECRET --tokenkey TOKEN_KEY --tokensecret TOKEN_SECRET &

## Search API program

Use all command line arguments to launch the program:

    python3 followers.py --id 0 --nodes 1 --couchip IP_GOES_HERE --consumerkey CONSUMER_KEY --consumersecret CONSUMER_SECRET --tokenkey TOKEN_KEY --tokensecret TOKEN_SECRET

Or as a background process (although it would be better to use a process manager):

    nohup python3 followers.py --id 0 --nodes 1 --couchip IP_GOES_HERE --consumerkey CONSUMER_KEY --consumersecret CONSUMER_SECRET --tokenkey TOKEN_KEY --tokensecret TOKEN_SECRET &
