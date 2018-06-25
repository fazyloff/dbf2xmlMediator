package net.fazyloff.wso2;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.soap.SOAPBody;
import org.apache.synapse.MessageContext; 
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.commons.codec.binary.Base64;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.xpath.AXIOMXPath;
import org.apache.axiom.soap.SOAPBody;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.AbstractMediator;
import org.jaxen.JaxenException;

import net.iryndin.jdbf.*;
import net.iryndin.jdbf.reader.DbfReader;
import net.iryndin.jdbf.core.DbfField;
import net.iryndin.jdbf.core.DbfMetadata;
import net.iryndin.jdbf.core.DbfRecord;
import net.iryndin.jdbf.reader.DbfReader;
import net.iryndin.jdbf.util.JdbfUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

@SuppressWarnings("unused")
public class DBF2XMLMediator extends AbstractMediator { 
	private static final String xmlRootName = "dbf";
	private String dbfEncoding = "Cp1251";
	private String xmlNs = "http://ws.apache.org/commons/ns/payload";
	private String dbfRecordName = "record";	
	private static final String nsPrefix = "ns";
	private static final String dbfFieldType = "datatype";
	private static final String dbfFieldLength = "len";
	private OMNamespace xmlNamespace;
	private OMFactory fac = OMAbstractFactory.getOMFactory();
	public boolean mediate(MessageContext context) { 
		this.xmlNamespace = fac.createOMNamespace(xmlNs, nsPrefix);
		SynapseLog logger = getLog(context);
		boolean traceOn = logger.isTraceEnabled();
	    boolean traceOrDebugOn = logger.isTraceOrDebugEnabled();
	    if (traceOrDebugOn) 
	    {
            logger.traceOrDebug("Start : dbf2xml mediator");

            if (traceOn) {
            	logger.traceOrDebug("Message : " + context.getEnvelope());
            }
        }
		SOAPBody body = context.getEnvelope().getBody();
		String base64encDBF;		
		OMElement binary = (OMElement) body.getFirstElement();			
		base64encDBF = binary.getText();
		byte[] decodedBytes = Base64.decodeBase64(base64encDBF.getBytes());
		InputStream inp = new ByteArrayInputStream(decodedBytes); 
		try {
			DbfReader dbfReader;
			dbfReader = new DbfReader(inp);			
			body.addChild(xmlFromDbfReader(dbfReader));
			binary.detach();
			dbfReader.close();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			if (traceOrDebugOn) 
		    {
	               logger.traceOrDebug(e.getMessage());
	               logger.traceOrDebug("End : dbf2xml mediator");
		    }		 
			 
		}			
		
		if (traceOrDebugOn) 
	    {
               logger.traceOrDebug("End : dbf2xml mediator");
	    }

		return true;
	}
	/**
	 * @return the dBFencoding
	 */
	/**
	 * @return the dbfEncoding
	 */
	public String getDbfEncoding() {
		return dbfEncoding;
	}
	/**
	 * @param dbfEncoding the dbfEncoding to set
	 */
	public void setDbfEncoding(String dbfEncoding) {
		this.dbfEncoding = dbfEncoding;
	}
/**
	 * @return the xmlNs
	 */
	public String getXmlNs() {
		return xmlNs;
	}
	/**
	 * @param xmlNs the xmlNs to set
	 */
	public void setXmlNs(String xmlNs) {
			this.xmlNs = xmlNs;
			this.xmlNamespace = fac.createOMNamespace(xmlNs, nsPrefix);
	}
	/**
	 * @return the dbfRecordName
	 */
	public String getDbfRecordName() {
		return dbfRecordName;
	}
	/**
	 * @param dbfRecordName the dbfRecordName to set
	 */
	public void setDbfRecordName(String dbfRecordName) {
		this.dbfRecordName = dbfRecordName;
	}
	private OMElement xmlFromDbfReader (DbfReader reader)
	{
		OMElement res = getNewOMElement(xmlRootName);
		 DbfRecord rec;
			try {
				while ((rec = reader.read()) != null)
				{				   
				     res.addChild(xmlFromDbfRecord(rec));
				}
			} catch (IOException e) {				
				e.printStackTrace();
			}		
		return res;
		
	}
	private OMElement xmlFromDbfRecord (DbfRecord rec)
	{
		OMElement res = getNewOMElement(dbfRecordName);
		rec.setStringCharset(Charset.forName(dbfEncoding));				
			 for (DbfField f : rec.getFields()) {
				 OMElement child = getNewOMElement(f.getName());				 
				 child.addAttribute(dbfFieldType, f.getType().toString(), xmlNamespace);
				 child.addAttribute(dbfFieldLength, String.valueOf(f.getLength()), xmlNamespace);
					  switch (f.getType()) {
		                case Character: {		                    
		                    String s = rec.getString(f.getName());
		                    if (s !=  null)
		                    {
		                    	child.setText(s);
		                    }
		                    break;
		                }
		                case Date: {
		                    Date d;
							try {
								d = rec.getDate(f.getName());
								if (d!= null){
								SimpleDateFormat dt1 = new SimpleDateFormat("yyyy-MM-dd");
								child.setText(dt1.format(d));
								}
							} catch (ParseException e) {
								e.printStackTrace();								
							}		                    	                    
		                    break;
		                }
		                case Numeric: {
		                    BigDecimal bd = rec.getBigDecimal(f.getName());
		                    if (bd != null){
		                    child.setText(bd.toPlainString());
		                    }
		                    break;
		                }
		                case Logical: {
		                    Boolean b = rec.getBoolean(f.getName());
		                    if (b != null){
		                    child.setText(rec.getString(f.getName()));
		                    }
		                    break;
		                }
		                default:
		                {
							child.setText(rec.getString(f.getName()));
							break;
		                }
		            }
					  res.addChild(child);
		        }
		return res;
}
	private OMElement getNewOMElement(String name)
	{		
		return fac.createOMElement(name, xmlNamespace);
	}	
}
