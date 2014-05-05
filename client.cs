// TODO:
//  * Handle subscribe/unsubscribe state change and support subscription callbacks
//  * Bulk calls (queue up commands and get callback with array of replies)

function RedisClient(%port, %host)
{
  return new ScriptObject()
  {
    class = "RedisClient";

    port = %port;
    host = %host;
  };
}

function RedisClient::onAdd(%this)
{
  if (%this.port $= "")
  {
    %this.port = 6379;
  }

  if (%this.host $= "")
  {
    %this.host = "127.0.0.1";
  }

  %this.parser = RedisReplyParser(%this);
  %this.callbacks = Queue().ref();

  %this.socket = new TCPObject(RedisClientTCP)
  {
    client = %this;
  };

  %this.socket.schedule(0, "connect");
}

function RedisClient::onRemove(%this)
{
  %this.callbacks.unref();
  %this.socket.delete();
}

function RedisClient::command(%this, %argv, %func, %data)
{
  if (!isCollection(%argv))
  {
    return 1;
  }

  %argv.ref();

  if (%this.socket.pendingDisconnect)
  {
    %argv.unref();
    return 1;
  }

  %payload = "*" @ %argv.size @ "\r\n";

  for (%i = 0; %i < %argv.size; %i++)
  {
    %item = %argv.item[%i];

    %payload = %payload @ "$" @ strLen(%item) @ "\r\n";
    %payload = %payload @ %item @ "\r\n";
  }

  %argv.unref();

  %this.socket.send(%payload);
  %this.callbacks.push(buildArray(%func, %data));

  return 0;
}

function RedisClientTCP::commandString(%this, %str, %func, %data)
{
  return %this.command(split(%str, " "), %func, %data);
}

function RedisClient::onReply(%this, %reply)
{
  %reply.ref();
  %entry = %this.callbacks.pop();

  %func = %entry.item[0];
  %data = %entry.item[1];

  if (isFunction(%func))
  {
    call(%func, %reply, %data, %this);
  }

  %reply.unref();

  if (%this.socket.pendingDisconnect && %this.callbacks.empty())
  {
    %this.socket.disconnect();
  }
}
