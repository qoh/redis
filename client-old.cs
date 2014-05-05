$Redis::CommandSent = 0;
$Redis::SendQueued = 1;
$Redis::InvalidArgs = 2;

function RedisClient(%port, %host, %options)
{
  if (%port $= "")
  {
    %port = 6379;
  }

  if (%host $= "")
  {
    %host = "127.0.0.1";
  }

  %obj = new ScriptObject()
  {
    class = "RedisClient";
    superClass = "ReferencedObject";

    port = %port;
    host = %host;
  };

  if (isMapping(%options))
  {
    %obj.options.update(%options);
    %options.ref().unref();
  }
  else if (%options !$= "")
  {
    error("ERROR: '" @ %options @ "' is not a valid mapping!");
  }

  return %obj;
}

function RedisClient::onAdd(%this)
{
  %this.state = "disconnected";
  %this.parser = RedisReplyParser(%this);

  %this.options = Map().ref();
  %this.callbackQueue = Queue().ref();
  %this.connectQueue = Queue().ref();

  %this.socket = new TCPObject(RedisClientTCP)
  {
    client = %this;
  };

  %this.socket.schedule(0, "connect");
}

function RedisClient::onRemove(%this)
{
  %this.options.unref();
  %this.callbackQueue.unref();
  %this.connectQueue.unref();

  %this.socket.disconnect();
  %this.socket.delete();
}

function RedisClient::onReply(%this, %data)
{
  if (%this.callbackQueue.empty())
  {
    error("ERROR: Got stray reply: " @ %data);
    return;
  }

  %command = %this.callbackQueue.pop();
  %callback = %command.item2;

  if (isFunction(%callback))
  {
    call(%callback, %data);
  }
}

// function RedisClient::command(%this, %v0, %v1, %v2, %v3, %v4, %v5, %v6, %v7, %v8, %v9)
// {
//   if (%this.state !$= "connected")
//   {
//     error("ERROR: RedisClient is not connected");
    
//     if (isCollection(%v0))
//     {
//       %v0.ref().unref();
//     }

//     return 0;
//   }

//   %data = %v0;

//   if (!isCollection(%data))
//   {
//     %data = _redis::makeArray(%v0, %v1, %v2, %v3, %v4, %v5, %v6, %v7, %v8, %v9);
//   }

//   %data.ref().unref();

//   if (!%data.size)
//   {
//     error("ERROR: At least one argument is required");
//     return 0;
//   }

//   %this.socket.send(_redis::pack(%data));
//   %this.commands.push(%data);

//   return 137;
// }

function RedisClient::command(%this, %command, %args, %callback, %noQueue)
{
  if (!isCollection(%args))
  {
    %args = split(%args, " ");
  }

  %args.ref();

  if (%command $= "")
  {
    error("ERROR: Command cannot be empty");
    %args.unref();
    return $Redis::InvalidArgs;
  }

  if (%callback !$= "" && !isFunction(%callback))
  {
    if (!%noQueue)
    {
      error("ERROR: Callback function '" @ %callback @ "' does not exist");
    }

    %args.unref();
    return $Redis::InvalidArgs;
  }

  %data = Array();

  %data.append(strUpr(%command));
  %data.append(%args);
  %data.append(%callback);

  if (%this.state !$= "connected")
  {
    if (!%noQueue)
    {
      %this.connectQueue.push(%data);
    }

    return $Redis::SendQueued;
  }

  %payload = Array().ref();

  %payload.append(strUpr(%command));
  %payload.concat(%args);

  %payload = _redis::pack(%payload.unref());

  %this.callbackQueue.push(%data);
  %this.socket.send(%payload);

  return $Redis::CommandSent;
}

function RedisClient::get(%this, %key, %callback)
{
  %args = Array();
  %args.append(%key);
  return %this.command("GET", %args, %callback);
}

function RedisClient::set(%this, %key, %value)
{
  %args = Array();
  %args.append(%key);
  %args.append(%value);
  return %this.command("SET", %args, %callback);
}

function RedisClient::del(%this, %key, %callback)
{
  %args = Array();
  %args.append(%key);
  return %this.command("DEL", %args, %callback);
}

function RedisClient::exists(%this, %key, callback)
{
  %args = Array();
  %args.append(%key);
  return %this.command("EXISTS", %args, %callback);
}

function RedisClient::incr(%this, %key, %callback)
{
  %args = Array();
  %args.append(%key);
  return %this.command("INCR", %args, %callback);
}

function RedisClient::incrby(%this, %key, %amount, %callback)
{
  %args = Array();
  %args.append(%key);
  %args.append(%amount);
  return %this.command("INCR", %args, %callback);
}