function RedisReplyParser(%client)
{
	return new ScriptObject()
	{
		class = "RedisReplyParser";
		client = %client;
	};
}

function RedisReplyParser::onAdd(%this)
{
	%this.lines = Queue().ref();
}

function RedisReplyParser::onRemove(%this)
{
	%this.lines.unref();
}

function RedisReplyParser::onLine(%this, %line)
{
	while (%line !$= "")
	{
		%line = nextToken(%line, "value", "\r\n");
		%this.lines.push(%value);
	}

	while (!%this.lines.empty())
	{
		%read = %this.readValue(0);

		if (%read $= "")
			break;

		%this.lines.shift(firstWord(%read));
		%this.client.onReply(restWords(%read));
	}
}

function RedisReplyParser::readValue(%this, %index)
{
	if (%index >= %this.lines.getCount())
		return "";

	%line = %this.lines.value[%index];
	%index++;

	%type = getSubStr(%line, 0, 1);
	%data = getSubStr(%line, 1, strLen(%line));

	if (%type $= "+" || %type $= "-" || %type $= ":")
	{
		if (%type $= ":")
			%data |= 0;

		if (%type $= "-")
			%data = "\c0" @ %data;

		return %index SPC %data;
	}

	if (%type $= "*")
	{
		if (%this.lines.getCount() - %index < %size)
			return "";

		%array = Array().ref();

		for (%i = 0; %i < %data; %i++)
		{
			%read = %this.readValue(%index);

			if (%read $= "")
			{
				%array.delete();
				return "";
			}

			%index = firstWord(%read);
			%array.append(restWords(%read));
		}

		return %index SPC %array.unref();
	}

	if (%type $= "$")
	{
		if (%data < 0)
			return %index SPC "";

		// now, we basically need to iterate through the upcoming lines until
		// reaching the intended size (concatenating them with \r\n), HACKHACK.
		%count = %this.lines.getCount();

		while (%index < %count && strLen(%bulk) < %data)
		{
			if (%bulk !$= "")
				%bulk = %bulk @ "\r\n";

			%bulk = %bulk @ %this.lines.value[%index];
			%index++;
		}

		if (strLen(%bulk) < %data)
			return "";

		return %index SPC getSubStr(%bulk, 0, %data);
	}

	%this.lines.clear();
	return "";
}