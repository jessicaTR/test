package com.thomsonreuters.ce.dbor.pasdi.cursor.struct;

import java.sql.ResultSet;
import java.sql.SQLException;
import com.thomsonreuters.ce.dbor.pasdi.cursor.struct.OracleObject.IDENTIFIER_VALUE_TYPE;

public class AstIdentifierValue  extends SDICursorRow{
	
	private static final long serialVersionUID = 1L;
	private IDENTIFIER_VALUE_TYPE IdentifierValue;
	
	public AstIdentifierValue(long Perm_ID,IDENTIFIER_VALUE_TYPE IdentifierValue) {
		super(Perm_ID);
		this.IdentifierValue = IdentifierValue;
	}
	
	public static AstIdentifierValue LoadDataFromRS(ResultSet RS) throws SQLException
	{
		
		IDENTIFIER_VALUE_TYPE idt_value= new IDENTIFIER_VALUE_TYPE(((oracle.sql.STRUCT)RS.getObject("v")).getAttributes());
		if(RS.wasNull())
		{
			idt_value=null;
		}		
		if(idt_value !=null ){
			long perm_id=idt_value.getIdentifier_entity_id();
			return new AstIdentifierValue(perm_id,idt_value);
		}
		return null;
	}
	
	public IDENTIFIER_VALUE_TYPE getIdentifierValue() {
		return IdentifierValue;
	}

}
