// TODO:
//  * Handle subscribe/unsubscribe state change and support subscription callbacks
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

function RedisClient::command(%this, %argv, %func, %data, %noAttachCallback)
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

  if (!%noAttachCallback)
  {
    %this.callbacks.push(buildArray(%func, %data));
  }

  return 0;
}

function RedisClientTCP::commandString(%this, %str, %func, %data)
{
  return %this.command(split(%str, " "), %func, %data);
}

function RedisClient::onReply(%this, %reply)
{
  %reply.ref();
  %entry = %this.callbacks.peek();

  // Check for multi
  if (%entry.item[2])
  {
    %entry.item[3].append(%reply);

    if (%entry.item[3].size >= %entry.item[2])
    {
      %this.callbacks.pop();
    }
    else
    {
      %reply.unref();
      return;
    }
  }
  else
  {
    %this.callbacks.pop();
  }

  %func = %entry.item[0];
  %data = %entry.item[1];

  if (isFunction(%func))
  {
    call(%func, %entry.item[2] ? %entry.item[3] : %reply, %data, %this);
  }

  %reply.unref();

  if (%this.socket.pendingDisconnect && %this.callbacks.empty())
  {
    %this.socket.disconnect();
  }
}

function RedisClient::multi(%this)
{
  return new ScriptObject()
  {
    class = "RedisClientMulti";
    client = %this;
  };
}

function RedisClientMulti::onAdd(%this)
{
  %this.commands = Array().ref;
}

function RedisClientMulti::onRemove(%this)
{
  %this.commands.unref();
}

function RedisClientMulti::exec(%this, %func, %data)
{
  if (%this.__exec)
  {
    return 1;
  }

  // This is where the magic happens
  %this.client.command(buildArray("MULTI"));

  // Issue each command that was queued
  for (%i = 0; %i < %this.commands.size; %i++)
  {
    %this.client.command(%this.commands.item[%i]);
  }
  
  // Attach a multi callback for the exec
  %this.client.callbacks.append(buildArray(
    %func, %data, %this.commands.size, Array()
  ));

  // Exec it without a callback
  %this.client.command(buildArray("EXEC"), "", "", 1);

  // That's it, folks — see you next time
  %this.__exec = 1;
  %this.schedule(0, "delete");

  return 0;
}

function RedisClientMulti::command(%this, %argv)
{
  if (!isCollection(%argv))
  {
    return 1;
  }

  %argv.ref();
  %this.commands.append(%argv);
  %argv.unref();

  return 0;
}

function RedisClientMulti::commandString(%this, %str)
{
  return %this.command(split(%str, " "));
}
