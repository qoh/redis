// Private: Create a new RedisClientSocket instance. The instance will use
// attributes from the given RedisClient.
//
// client - An instance of a RedisClient.
//
// Returns the new instance.
function RedisClientSocket(%client)
{
  return new TCPObject(RedisClientSocket)
  {
    client = %client;
  };
}

// Internal: Initialize a new RedisClientSocket.
//
// Returns nothing.
function RedisClientSocket::onAdd(%this)
{
  %this.connected = 0;
  %this.pendingDisconnect = 0;
}

// Public: Start connecting to the server indicated by the client properties.
// Nothing will happen if the client is already connected.
//
// Returns nothing.
function RedisClientSocket::connect(%this)
{
  if (!%this.connected)
  {
    %this.pendingDisconnect = 0;
    Parent::connect(%this, %this.client.host @ ":" @ %this.client.port);
  }
}

// Public: Request a disconnect from the Redis server. An actual disconnect
// will be done once the callback queue is empty. No more commands can be
// issued after requesting a disconnect.
//
// Returns nothing.
function RedisClientSocket::disconnect(%this)
{
  if (%this.client.callbacks.empty())
  {
    Parent::disconnect(%this);
    %this.onDisconnect();
  }
  else
  {
    %this.pendingDisconnect = 1;
  }
}

// Internal: Sends arbitrary data to the server or queues it if not connected.
//
// data - The data to send.
//
// Returns nothing.
function RedisClientSocket::send(%this, %data)
{
  if (%this.connected)
  {
    Parent::send(%this, %data);
  }
  else
  {
    %this.queue = %this.queue @ %data;
  }
}

// Internal: Handle a connection being established.
//
// Returns nothing.
function RedisClientSocket::onConnected(%this)
{
  %this.connected = 1;
  %this.pendingDisconnect = 0;

  if (%this.queue !$= "")
  {
    %this.send(%this.queue);
    %this.queue = "";
  }
}

// Internal: Handle a disconnect.
//
// Returns nothing.
function RedisClientSocket::onDisconnect(%this)
{
  %this.connected = 0;
  %this.pendingDisconnect = 0;
}

// Internal: Send the incoming line to the RedisReplyParser.
//
// line - The received line.
//
// Returns nothing.
function RedisClientSocket::onLine(%this, %line)
{
  %this.client.parser.onLine(%line);
}
