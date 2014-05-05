function _redis::pack(%data)
{
  if (isCollection(%data))
  {
    %payload = "*" @ %data.size @ "\r\n";

    for (%i = 0; %i < %data.size; %i++)
    {
      %payload = %payload @ _redis::pack(%data.item[%i]);
    }

    return %payload;
  }

  // If %data contains a newline, a bulk string is the only safe method.
  if (1 || strPos(%data, "\r\n") != -1)
  {
    return "$" @ strLen(%data) @ "\r\n" @ %data @ "\r\n";
  }

  // Now, magically figure out if the user wants %data to be a string or
  // an integer! Let's just assume string and see how much breaks.
  return "+" @ %data @ "\r\n";
}

exec("./parser.cs");
exec("./socket.cs");
exec("./client.cs");