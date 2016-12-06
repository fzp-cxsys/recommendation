package freagle.recommendation.bean;

import java.util.Date;

/**
 * Created by freagle on 16-12-3.
 */
public class Item {
    private int item;
    private String title;
    private String url;
    private String genres;

    public int getItem() {
        return item;
    }

    public void setItem(int item) {
        this.item = item;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }
}
