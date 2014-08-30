// Public: Create a new RedisClient instance which connects to the specified
// server.
//
// port - The integer port to connect to. (default: 6379)
// host - The string address to connect to. (default: "127.0.0.1")
//
// Returns the new instance.
function RedisClient(%port, %host)
{
  // Fill in default values for empty arguments
  if (%port $= "")
    %port = 6379;

  if (%host $= "")
    %host = "127.0.0.1";

  return new ScriptObject()
  {
    class = "RedisClient";

    port = %port;
    host = %host;
  };
}

// Internal: Initialize a new RedisClient.
//
// Returns nothing.
function RedisClient::onAdd(%this)
{
  // Create classes mapped to the client
  %this.parser = RedisReplyParser(%this);
  %this.socket = RedisClientSocket(%this);

  %this.callbacks = Queue().ref();
  %this.subscriptions = Map().ref();

  // Start connecting on the next frame, so the user has a chance to replace
  // member fields and possibly change the port/host using the attributes.
  %this.socket.schedule(0, "connect");
}

// Internal: Deconstruct a RedisClient.
//
// Returns nothing.
function RedisClient::onRemove(%this)
{
  // Don't delete, just unref. Something else might have a reference to them.
  %this.subscriptions.unref();
  %this.callbacks.unref();

  // Should perhaps force a emergency disconnect here?
  %this.socket.delete();
}

// Internal: Handle an incoming parsed reply from the RedisReplyParser
// instance.
//
// Returns nothing.
function RedisClient::onReply(%this, %reply)
{
  %reply.ref();

  // If there are no callbacks, either something is wrong or we're receiving
  // subscription replies ...
  if (%this.callbacks.empty())
  {
    // ... if that is the case, make sure we check them
    if (%reply.item[0] $= "message" &&
      %this.subscriptions.exists(%reply.item[1]))
    {
      %entries = %this.subscriptions.get(%reply.item[1]);

      // Call every mapped callback
      for (%i = 0; %i < %entries.size; %i++)
      {
        %entry = %entries.item[%i];

        %func = %entries.item[%i].item[0];
        %data = %entries.item[%i].item[1];

        // It might just be an empty callback
        if (isFunction(%func))
          call(%func, %reply.item[2], %data, %this);
      }
    }

    %reply.unref();
    return;
  }

  // Pop it, don't drop it. If it's a multi we'll keep it there until the
  // required number of replies is satisfied.
  %entry = %this.callbacks.peek();

  // Check for multi callbacks. These are special in that the third element is
  // a positive number of replies required. Normal callbacks only have two
  // elements.
  if (%entry.item[2])
  {
    // For these, the final element is an Array used for accumulating the
    // replies.
    %entry.item[3].append(%reply);

    // If the Array has enough elements, just pop it and run the callback code
    // for normal callbacks! Simple ;)
    if (%entry.item[3].size >= %entry.item[2])
    {
      %this.callbacks.pop();
    }
    else
    {
      // If we don't have enough replies yet, keep it in the Queue and do
      // nothing else in particular.
      %reply.unref();
      return;
    }
  }
  else
  {
    // Normal callback, just pop it anyway.
    %this.callbacks.pop();
  }

  %func = %entry.item[0];
  %data = %entry.item[1];

  // Callbacks can refer to empty functions. This is to keep the order in the
  // Queue in respect to the Redis server's replies.
  if (isFunction(%func))
  {
    // If it's a multi callback, use the fourth element. Otherwise, just pass
    // the reply data like normal.
    call(%func, %entry.item[2] ? %entry.item[3] : %reply, %data, %this);
  }

  %reply.unref();

  // Did the user request a disconnect? If there's no more callbacks, try now.
  if (%this.socket.pendingDisconnect && %this.callbacks.empty())
    %this.socket.disconnect();
}

// Public: Issue a new command to the Redis server using an Array of values.
// This is the best for commands that employ arbitrary input of some degree, as
// simply using `::commandString()` could open up for vulnerabilities similar
// to SQL injection.
//
// argv             - An Array of strings to send to the server.
// func             - An optional function to call once the command reply is
//                    received.
// data             - A value which will be passed to *func*, in addition to
//                    the reply of the command.
// noAttachCallback - Don't add a callback to the queue. Use only for commands
//                    that do not trigger a straight-forward reply, like EXEC.
//                    Defaults to false.
//
// Returns 0 on success, 1 otherwise.
function RedisClient::command(%this, %argv, %func, %data, %noAttachCallback)
{
  if (!isCollection(%argv))
    return 1;

  %argv.ref();

  // Don't allow new commands if the user requested a disconnect.
  if (%this.socket.pendingDisconnect)
  {
    %argv.unref();
    return 1;
  }

  // Build the text payload to send to the Redis server. *argv* is serialized
  // as a single RESP array, containing RESP bulk strings.
  %payload = "*" @ %argv.size @ "\r\n";

  for (%i = 0; %i < %argv.size; %i++)
  {
    %item = %argv.item[%i];

    %payload = %payload @ "$" @ strLen(%item) @ "\r\n";
    %payload = %payload @ %item @ "\r\n";
  }

  %argv.unref();

  // The RedisClientSocket implementation will automatically queue sent data
  // for when the socket connects and is ready to do so, so there's no need to
  // check it here. The callback entry is appended regardless of state.
  %this.socket.send(%payload);

  if (!%noAttachCallback)
    %this.callbacks.append(buildArray(%func, %data));

  return 0;
}

// Public: Issue a new command to the Redis server using a plain string
// representation. The given string will be split into an Array by each word.
//
// argv             - A string which will be split into individual words.
// func             - An optional function to call once the command reply is
//                    received.
// data             - A value which will be passed to *func*, in addition to
//                    the reply of the command.
// noAttachCallback - Don't add a callback to the queue. Use only for commands
//                    that do not trigger a straight-forward reply, like EXEC.
//                    Defaults to false.
//
// Returns 0 on success, 1 otherwise.
function RedisClient::commandString(%this, %str, %func, %data, %noAttachCallback)
{
  return %this.command(split(%str, " "), %func, %data, %noAttachCallback);
}

// Public: Tell the Redis server to start sending messages published on the
// specified channels. If a valid callback function is passed, it will be
// called with each message received. No normal commands can be issued while
// subscribed.
//
// channels - Either an Array or a space-separated list of channel names to
//            subscribe the callback function to.
// func     - A function to call for each message received from the channels.
// data     - A value which will be passed to *func* for each message, in
//            addition to the value of the message.
//
// Returns nothing.
function RedisClient::subscribe(%this, %channels, %func, %data)
{
  if (!isCollection(%channels))
    %channels = split(%channels, " ");

  // Just use `::command()` like normal to indicate the intent of subscribing.
  %argv = buildArray("SUBSCRIBE");
  %argv.concat(%channels.ref().unref());

  %this.command(%argv);

  for (%i = 0; %i < %channels.size; %i++)
  {
    // This assigns an empty Array to the value of the channel in the map if it
    // was not already set. Finally sets *channel* to the resulting Array.
    %channel = %this.subscriptions.setDefault(
      %channels.item[%i],
      Array().ref().unref()
    );

    // If there's no callback function, simply don't bother with it. The
    // presence of the channel key as an Array is enough for the subscription
    // to work in `::onReply()` regardless.
    if (%func $= "")
      continue;

    // Remove all callback entries with the same function. This is both to
    // allow changing the data used with the function, as well as to avoid
    // duplicate callbacks.
    for (%j = 0; %j < %channel.size; %i++)
    {
      %entry = %channel.item[%j];

      if (%entry.item[0] $= %func)
      {
        %entry.pop(%j);
        %j--;
      }
    }

    // As all instances of the function have been removed, add the entry to the
    // callback map unconditionally.
    %channel.append(buildArray(%func, %data));
  }
}

// Public: Remove an active subscription from the given set of channels. The
// Redis server will be told to stop sending messages from them, and all
// callback functions mapped to the channels will be removed.
//
// channels - Either an Array or a space-separated list of channel names to
//            unsubscribe from.
//
// Returns nothing.
function RedisClient::unsubscribe(%this, %channels)
{
  if (!isCollection(%channels))
    %channels = split(%channels, " ");

  for (%i = %this.channels.size - 1; %i >= 0; %i--)
  {
    // `::remove()` works even if the key doesn't exist.
    %this.subscriptions.remove(%channels.item[%i]);
  }

  // TODO: The UNSUBSCRIBE command may actually send a reply for every channel
  // that the client unsubscribed from. Will have to test further, as this only
  // consumes a single reply.
  %argv = buildArray("UNSUBSCRIBE");
  %argv.concat(%channels.ref().unref());

  %this.command(%argv);
}

// Public: Create a new MULTI command execution context.
//
// Returns the new RedisClientMulti instance.
function RedisClient::multi(%this)
{
  return new ScriptObject()
  {
    class = "RedisClientMulti";
    client = %this;
  };
}

// Internal: Initialize a new RedisClientMulti instance.
//
// Returns nothing.
function RedisClientMulti::onAdd(%this)
{
  %this.commands = Array().ref();
}

// Internal: Deconstruct a RedisClientMulti instance.
//
// Returns nothing.
function RedisClientMulti::onRemove(%this)
{
  %this.commands.unref();
}

// Public: Send the queued up sequence of commands to the Redis server. The
// callback will be given an Array with the reply from each command, in order.
//
// func - An optional function to call with the results of the MULTI.
// data - A value which will be passed to *func* in addition to the results.
//
// Returns nothing.
function RedisClientMulti::exec(%this, %func, %data)
{
  // This is where the magic happens
  %this.client.command(buildArray("MULTI"));

  // Issue each command that was queued
  for (%i = 0; %i < %this.commands.size; %i++)
    %this.client.command(%this.commands.item[%i]);

  // Attach a multi callback for the exec
  %this.client.callbacks.append(buildArray(
    %func, %data, %this.commands.size, Array()
  ));

  // Exec it without a callback
  %this.client.command(buildArray("EXEC"), "", "", 1);

  // That's it, folks — see you next time
  %this.delete();
}

// Public: Add a command to the MULTI queue. See `RedisClient::command` for
// more detail.
//
// argv - See `RedisClient::command`.
//
// Returns 0 on success, 1 otherwise.
function RedisClientMulti::command(%this, %argv)
{
  if (!isCollection(%argv))
    return 1;

  %argv.ref();
  %this.commands.append(%argv);
  %argv.unref();

  return 0;
}

// Public: Add a command to the queue using a space-separated string. See
// `RedisClient::commandString` for more detail.
//
// argv - See `RedisClient::commandString`.
//
// Returns 0 on success, 1 otherwise.
function RedisClientMulti::commandString(%this, %str)
{
  return %this.command(split(%str, " "));
}