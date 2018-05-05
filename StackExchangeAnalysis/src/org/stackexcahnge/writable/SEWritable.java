package org.stackexcahnge.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SEWritable implements Writable {

	private int postType;
	private int parentId;
	private String dateCreatedUpdated;
	private int commentsCount;
	private String postText;
	
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.postType=dataInput.readInt();
		this.parentId=dataInput.readInt();
		this.dateCreatedUpdated=dataInput.readUTF();
		this.commentsCount=dataInput.readInt();
		this.postText=dataInput.readUTF();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(this.postType);
		dataOutput.writeInt(this.parentId);
		dataOutput.writeUTF(this.dateCreatedUpdated);
		dataOutput.writeInt(this.commentsCount);
		dataOutput.writeUTF(this.postText);
	}

	public SEWritable(String postData) {
		String[] postDataParts=postData.split(",");
		this.postType=Integer.valueOf(postDataParts[0]);
		if(this.postType==2) {
			this.parentId=Integer.valueOf(postDataParts[1]);
		}
		this.dateCreatedUpdated=postDataParts[2];
		this.commentsCount=Integer.valueOf(postDataParts[3]);
		
		StringBuilder text = new StringBuilder();
		for (int i = 4; i < postDataParts.length; i++) {
			text.append(",");
			text=text.append(postDataParts[i]);
		}
		this.postText=text.toString();
	}
	
	public SEWritable() {}
	
	public int getPostType() {
		return postType;
	}

	public void setPostType(int postType) {
		this.postType = postType;
	}

	public int getParentId() {
		return parentId;
	}

	public void setParentId(int parentId) {
		this.parentId = parentId;
	}

	public int getCommentsCount() {
		return commentsCount;
	}

	public void setCommentsCount(int commentsCount) {
		this.commentsCount = commentsCount;
	}

	public String getPostText() {
		return postText;
	}

	public void setPostText(String postText) {
		this.postText = postText;
	}

	public String getDateCreatedUpdated() {
		return dateCreatedUpdated;
	}

	public void setDateCreatedUpdated(String dateCreatedUpdated) {
		this.dateCreatedUpdated = dateCreatedUpdated;
	}
}
