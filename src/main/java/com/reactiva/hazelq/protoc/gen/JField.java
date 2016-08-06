package com.reactiva.hazelq.protoc.gen;

import org.springframework.util.StringUtils;

public class JField {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JField other = (JField) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	public JField(String name, Class<?> type) {
		super();
		this.name = StringUtils.uncapitalize(StringUtils.trimAllWhitespace(name));
		this.type = type;
	}
	final String name;
	final Class<?> type;
}
