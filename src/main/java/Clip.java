public class Clip {

    private String id;
    private String url;
    private String embedURL;
    private String broadcasterID;
    private String broadcasterName;
    private String creatorID;
    private String creatorName;
    private String gameID;
    private String title;
    private long viewCount;
    private String createdAt;
    private String thumbnailImageURL;

    public Clip(String id, String url, String embedURL, String broadcasterID, String broadcasterName, String creatorID,
                String creatorName, String gameID, String title, long viewCount, String createdAt, String thumbnailImageURL) {
        this.id = id;
        this.url = url;
        this.embedURL = embedURL;
        this.broadcasterID = broadcasterID;
        this.broadcasterName = broadcasterName;
        this.creatorID = creatorID;
        this.creatorName = creatorName;
        this.gameID = gameID;
        this.title = title;
        this.viewCount = viewCount;
        this.createdAt = createdAt;
        this.thumbnailImageURL = thumbnailImageURL;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getEmbedURL() {
        return embedURL;
    }

    public void setEmbedURL(String embedURL) {
        this.embedURL = embedURL;
    }

    public String getBroadcasterID() {
        return broadcasterID;
    }

    public void setBroadcasterID(String broadcasterID) {
        this.broadcasterID = broadcasterID;
    }

    public String getBroadcasterName() {
        return broadcasterName;
    }

    public void setBroadcasterName(String broadcasterName) {
        this.broadcasterName = broadcasterName;
    }

    public String getCreatorID() {
        return creatorID;
    }

    public void setCreatorID(String creatorID) {
        this.creatorID = creatorID;
    }

    public String getCreatorName() {
        return creatorName;
    }

    public void setCreatorName(String creatorName) {
        this.creatorName = creatorName;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String getThumbnailImageURL() {
        return thumbnailImageURL;
    }

    public void setThumbnailImageURL(String thumbnailImageURL) {
        this.thumbnailImageURL = thumbnailImageURL;
    }

    public String getGameID() {
        return gameID;
    }

    public void setGameID(String gameID) {
        this.gameID = gameID;
    }
}
