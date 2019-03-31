package cs455.hadoop.util;

import java.util.Comparator;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;

public enum SongData {

  HOTNESS
  {
    @Override
    public Comparator<Entry<Text, Song>> comparator() {
      return (e1, e2) -> e1.getValue().getHotness()
          .compareTo( e2.getValue().getHotness() );
    }

    @Override
    public void add(Song song, double value) {
      if ( value != 0 )
      {
        song.setHotness( value );
      }
    }

    @Override
    public boolean isInvalid(Song song) {
      return song.getHotness() != cs455.hadoop.util.Song.EPSILON;
    }

    @Override
    public Double getValue(Song song) {
      return song.getHotness();
    }
  },
  DURATION
  {
    @Override
    public Comparator<Entry<Text, Song>> comparator() {
      return (e1, e2) -> e1.getValue().getDuration()
          .compareTo( e2.getValue().getDuration() );
    }

    @Override
    public void add(Song song, double value) {
      if ( value != 0 )
      {
        song.setDuration( value );
      }
    }

    @Override
    public boolean isInvalid(Song song) {
      return song.getDuration() != cs455.hadoop.util.Song.EPSILON;
    }

    @Override
    public Double getValue(Song song) {
      return song.getDuration();
    }
  },
  DANCE_ENERGY
  {
    @Override
    public Comparator<Entry<Text, Song>> comparator() {
      return (e1, e2) -> e1.getValue().getDancergy()
          .compareTo( e2.getValue().getDancergy() );
    }

    @Override
    public void add(Song song, double value) {
      if ( value != 0 )
      {
        song.setDancergy( value );
      }
    }

    @Override
    public boolean isInvalid(Song song) {
      return song.getDancergy() != cs455.hadoop.util.Song.EPSILON;
    }

    @Override
    public Double getValue(Song song) {
      return song.getDancergy();
    }
  };

  public abstract Comparator<Entry<Text, Song>> comparator();

  public abstract void add(Song Song, double value);

  public abstract boolean isInvalid(Song song);

  public abstract Double getValue(Song song);
}
