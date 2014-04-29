package com.thomsonreuters.ce.timing;

public class ScheduleType {
	public final static ScheduleType MONTHLY = new ScheduleType('M');

	public final static ScheduleType WEEKLY = new ScheduleType('W');

	public final static ScheduleType DAILY = new ScheduleType('D');

	public final static ScheduleType HOURLY = new ScheduleType('H');

	public final static ScheduleType MINUTELY = new ScheduleType('m');
	
	public final static ScheduleType ONEOFF = new ScheduleType('O');

	protected char TypeIndicator;

	private ScheduleType(char Indicator) {
		this.TypeIndicator = Indicator;
	}

	public char GetIndicator() {
		return this.TypeIndicator;
	}

	public static ScheduleType getInstance(char Indicator) {
		switch (Indicator) {
		case 'M':
			return MONTHLY;
		case 'W':
			return WEEKLY;
		case 'D':
			return DAILY;
		case 'H':
			return HOURLY;
		case 'm':
			return MINUTELY;			
		case 'O':
			return ONEOFF;			
		default:
			return new ScheduleType(Indicator);
		}
	}

	public boolean equals(Object x) {
		if (x instanceof ScheduleType) {
			if (((ScheduleType) x).GetIndicator() == this.TypeIndicator) {
				return true;
			}
		}
		return false;
	}

	public int hashCode() {
		return String.valueOf(this.TypeIndicator).hashCode();
	}
}
