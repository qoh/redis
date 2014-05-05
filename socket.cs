function RedisClientTCP::onAdd(%this)
{
  %this.connected = 0;
  %this.pendingDisconnect = 0;
}

function RedisClientTCP::connect(%this)
{
  if (!%this.connected)
  {
    %this.pendingDisconnect = 0;
    Parent::connect(%this, %this.client.host @ ":" @ %this.client.port);
  }
}

function RedisClientTCP::disconnect(%this)
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

function RedisClientTCP::send(%this, %data)
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

function RedisClientTCP::onConnected(%this)
{
  %this.connected = 1;
  %this.pendingDisconnect = 0;

  %this.send(%this.queue);
  %this.queue = "";
}

function RedisClientTCP::onDisconnect(%this)
{
  %this.connected = 0;
  %this.pendingDisconnect = 0;
}

function RedisClientTCP::onLine(%this, %line)
{
  %this.client.parser.onLine(%line);
}