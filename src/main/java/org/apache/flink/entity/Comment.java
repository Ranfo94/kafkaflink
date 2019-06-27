package org.apache.flink.entity;

public class Comment {

    private long approveDate;
    private String articleID;
    private int articleWordCount;
    private long commentID;
    private String commentType;
    private long createDate;
    private short depth;
    private boolean editorsSelection;
    private long inReplyTo;
    private String parentUserDisplayName;
    private int recomendations;
    private String sectionName;
    private String userDisplayName;
    private long userID;
    private String userLocation;

    public Comment(){}

    public static Comment fromString(String s){
        Comment comment = new Comment();

        String[] fields = cleanAndSplitString(s);
        try {

            if (!fields[0].equals("null"))
                comment.approveDate = Long.parseLong(fields[0]);
            if (!fields[1].equals("null"))
                comment.articleID = fields[1];
            if (!fields[2].equals("null"))
                comment.articleWordCount = Integer.parseInt(fields[2]);
            if (!fields[3].equals("null"))
                comment.commentID = Long.parseLong(fields[3]);
            if (!fields[4].equals("null"))
                comment.commentType = fields[4];
            if (!fields[5].equals("null"))
                comment.createDate = Long.parseLong(fields[5]);
            if (!fields[6].equals("null"))
                comment.depth = Short.parseShort(fields[6]);
            if (!fields[7].equals("null"))
                comment.editorsSelection = Boolean.getBoolean(fields[7]);
            if (!fields[8].equals("null"))
                comment.inReplyTo = Long.parseLong(fields[8]);
            if (!fields[9].equals("null"))
                comment.parentUserDisplayName = fields[9];
            if (!fields[10].equals("null"))
                comment.recomendations = Integer.parseInt(fields[10]);
            if (!fields[11].equals("null"))
                comment.sectionName = fields[11];
            if (!fields[12].equals("null"))
                comment.userDisplayName = fields[12];
            if (!fields[13].equals("null"))
                comment.userID = Long.parseLong(fields[13]);
            if (!fields[14].equals("null"))
                comment.userLocation = fields[14];
        }catch (NumberFormatException nf){
            System.err.println("ELIMINO UN COMMENTO NON VALIDO!");
            //nf.printStackTrace();
            return null;
        }
        catch (Exception e){
            e.printStackTrace();
            //throw new  RuntimeException("INVALID ENTRY : "+s);
        }
        return comment;
    }

    private static String[] cleanAndSplitString(String s){
        String[] preproc = s.split("\"");
        for (int i=1;i<preproc.length;i+=2){
            preproc[i]=preproc[i].replace(",","");
        }
        String s1 = String.join("",preproc);

        return s1.split(",");

        //String location = "";
        //solve city field
        /*for (int i=14;i<fields.length;i++){
            location += fields[i];
        }
        location=location.replace("\"","");
        fields[14]=location;*/
    }

    @Override
    public String toString() {
        return "Comment{" +
                "approveDate=" + approveDate +
                ", articleID='" + articleID + '\'' +
                ", articleWordCount=" + articleWordCount +
                ", commentID=" + commentID +
                ", commentType='" + commentType + '\'' +
                ", createDate=" + createDate +
                ", depth=" + depth +
                ", editorsSelection=" + editorsSelection +
                ", inReplyTo=" + inReplyTo +
                ", parentUserDisplayName='" + parentUserDisplayName + '\'' +
                ", recomendations=" + recomendations +
                ", sectionName='" + sectionName + '\'' +
                ", userDisplayName='" + userDisplayName + '\'' +
                ", userID=" + userID +
                ", userLocation='" + userLocation + '\'' +
                '}';
    }

    public long getApproveDate() {
        return approveDate;
    }

    public void setApproveDate(long approveDate) {
        this.approveDate = approveDate;
    }

    public String getArticleID() {
        return articleID;
    }

    public void setArticleID(String articleID) {
        this.articleID = articleID;
    }

    public int getArticleWordCount() {
        return articleWordCount;
    }

    public void setArticleWordCount(int articleWordCount) {
        this.articleWordCount = articleWordCount;
    }

    public long getCommentID() {
        return commentID;
    }

    public void setCommentID(long commentID) {
        this.commentID = commentID;
    }

    public String getCommentType() {
        return commentType;
    }

    public void setCommentType(String commentType) {
        this.commentType = commentType;
    }

    public long getCreateDate() {
        return createDate;
    }

    public void setCreateDate(long createDate) {
        this.createDate = createDate;
    }

    public short getDepth() {
        return depth;
    }

    public void setDepth(short depth) {
        this.depth = depth;
    }

    public boolean isEditorsSelection() {
        return editorsSelection;
    }

    public void setEditorsSelection(boolean editorsSelection) {
        this.editorsSelection = editorsSelection;
    }

    public long getInReplyTo() {
        return inReplyTo;
    }

    public void setInReplyTo(long inReplyTo) {
        this.inReplyTo = inReplyTo;
    }

    public String getParentUserDisplayName() {
        return parentUserDisplayName;
    }

    public void setParentUserDisplayName(String parentUserDisplayName) {
        this.parentUserDisplayName = parentUserDisplayName;
    }

    public int getRecomendations() {
        return recomendations;
    }

    public void setRecomendations(int recomendations) {
        this.recomendations = recomendations;
    }

    public String getSectionName() {
        return sectionName;
    }

    public void setSectionName(String sectionName) {
        this.sectionName = sectionName;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public long getUserID() {
        return userID;
    }

    public void setUserID(long userID) {
        this.userID = userID;
    }

    public String getUserLocation() {
        return userLocation;
    }

    public void setUserLocation(String userLocation) {
        this.userLocation = userLocation;
    }
}
