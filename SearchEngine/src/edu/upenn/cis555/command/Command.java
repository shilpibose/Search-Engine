package edu.upenn.cis555.command;

public class Command {

	protected CommandType commandType;

	public Command (CommandType type) {
		commandType = type;
	}

	public CommandType getCommandType () {
		return commandType;
	}
}
