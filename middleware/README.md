# Middleware

I created a build for testing purposes. The final websocket will be at '/ws'. 
But for testing at '/testing' there is a stream of dummy data out of the 'measurements-out-short.csv' file.

I have build it and you should be able to just do './api' from inside the middleware file (ignore the logs they are for the actual web socket not the test one).
The testing websockets sends data every 50ms and right now this is not changable if you need it to be changable just write me (Alex).

If that dosent work you need to install go and do `go run ./cmd/api` from inside the middleware file.