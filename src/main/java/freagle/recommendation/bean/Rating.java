package freagle.recommendation.bean;

import java.sql.Timestamp;

/**
 * Created by freagle on 16-12-3.
 */
public class Rating {
    private int user;
    private int item;
    private double rating;
    private Timestamp ratingTime;

    public int getUser() {
        return user;
    }

    public void setUser(int user) {
        this.user = user;
    }

    public int getItem() {
        return item;
    }

    public void setItem(int item) {
        this.item = item;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public Timestamp getRatingTime() {
        return ratingTime;
    }

    public void setRatingTime(Timestamp ratingTime) {
        this.ratingTime = ratingTime;
    }
}
