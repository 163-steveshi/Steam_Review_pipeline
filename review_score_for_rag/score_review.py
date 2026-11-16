from sqlalchemy import create_engine, text, bindparam, Text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.orm import Session, sessionmaker
import os
from score import score_review
from dotenv import load_dotenv

load_dotenv("./.env")


def main():

    DATABASE_URL_LOCAL = os.environ["DATABASE_URL_LOCAL"]
    engine = create_engine(DATABASE_URL_LOCAL)
    SessionLocal = sessionmaker(bind=engine, autoflush=True)
    with SessionLocal() as session:
        query = text("""
                     SELECT review.app_id, review.review_id, review.source_raw_id,
                      game.source_raw_id as game_source_raw_id, game.name as game_name,
                      game.app_type, game.genres, review.review_text, review.language,
                      review.voted_positive,
                        review.steam_purchase,
                        review.received_for_free,
                        review.written_during_early_access,
                        review.primarily_play_on_steam_deck,

                        -- Playtime & Timestamps
                        player_info.playtime_at_review_mins,
                        review.timestamp_created,
                        review.timestamp_updated,

                        -- Trust & Ranking Signals
                        review.votes_helpful,
                        review.votes_funny,
                        review.weighted_vote_score

                      
                    FROM silver.clean_latest_review as review
                    INNER JOIN silver.clean_latest_review_player_info as player_info
                      on review.review_id = player_info.review_id and review.app_id = player_info.app_id
                    INNER JOIN silver.dim_game as game
                      on review.app_id = game.app_id
                    WHERE review.app_id = :app_id 
                     """)

        # 2. Execute and pass the parameter value
        rows = session.execute(query, {"app_id": 4001890}).mappings().all()
        records = [dict(r) for r in rows]
        print(len(records))
        for record in records:
            result = score_review(record["review_text"], record["language"])
            record["word_count"] = result["token_count"]
            record["unique_word_ratio"] = result["unique_word_count"]
            record["letter_ratio"] = result["alpha_ratio"] or 0
            record["has_url"] = result["has_url"]
            record["reject_reason"] = result["reject_reason"]
            record["text_hash"] = result["text_hash"]
            record["unique_word_ratio"] = result["unique_word_ratio"]
            record["scoring_version"] = result["scoring_version"]
            record["has_char_spam"] = "char_spam" in result["reject_reason"]
            record["review_text"] = result["clean_text"]
            record["dup_rank"] = -1
        UPSERT_REVIEW_RAG_SCORE = text("""
            INSERT INTO gold.review_rag_score (
                app_id, review_id, source_raw_id, game_source_raw_id,
                game_name, app_type, genres,
                review_text, language,
                voted_positive, timestamp_created, timestamp_updated,
                steam_purchase, received_for_free,
                written_during_early_access, primarily_play_on_steam_deck,
                playtime_at_review_mins,
                votes_helpful, votes_funny, weighted_vote_score,
                word_count, unique_word_ratio, letter_ratio,
                has_url, has_char_spam, text_hash,   dup_rank,
                reject_reason, scoring_version
            )
            VALUES (
                :app_id, :review_id, :source_raw_id, :game_source_raw_id,
                :game_name, :app_type, :genres,
                :review_text, :language,
                :voted_positive, :timestamp_created, :timestamp_updated,
                :steam_purchase, :received_for_free,
                :written_during_early_access, :primarily_play_on_steam_deck,
                :playtime_at_review_mins,
                :votes_helpful, :votes_funny, :weighted_vote_score,
                :word_count, :unique_word_ratio, :letter_ratio,
                :has_url, :has_char_spam, :text_hash, :dup_rank,
                :reject_reason, :scoring_version
            )
            ON CONFLICT (app_id, review_id) DO UPDATE SET
                source_raw_id                = EXCLUDED.source_raw_id,
                game_source_raw_id           = EXCLUDED.game_source_raw_id,
                game_name                    = EXCLUDED.game_name,
                app_type                     = EXCLUDED.app_type,
                genres                       = EXCLUDED.genres,
                review_text                  = EXCLUDED.review_text,
                language                     = EXCLUDED.language,
                voted_positive               = EXCLUDED.voted_positive,
                timestamp_created            = EXCLUDED.timestamp_created,
                timestamp_updated            = EXCLUDED.timestamp_updated,
                steam_purchase               = EXCLUDED.steam_purchase,
                received_for_free            = EXCLUDED.received_for_free,
                written_during_early_access  = EXCLUDED.written_during_early_access,
                primarily_play_on_steam_deck = EXCLUDED.primarily_play_on_steam_deck,
                playtime_at_review_mins      = EXCLUDED.playtime_at_review_mins,
                votes_helpful                = EXCLUDED.votes_helpful,
                votes_funny                  = EXCLUDED.votes_funny,
                weighted_vote_score          = EXCLUDED.weighted_vote_score,
                word_count                   = EXCLUDED.word_count,
                unique_word_ratio            = EXCLUDED.unique_word_ratio,
                letter_ratio                 = EXCLUDED.letter_ratio,
                has_url                      = EXCLUDED.has_url,
                has_char_spam                = EXCLUDED.has_char_spam,
                text_hash                    = EXCLUDED.text_hash,
                reject_reason                = EXCLUDED.reject_reason,
                scoring_version              = EXCLUDED.scoring_version,
                scored_at                    = now()
        """).bindparams(
            bindparam("genres", type_=JSONB),  # Python list/dict -> jsonb
            bindparam(
                "reject_reason", type_=ARRAY(Text)
            ),  # Python list[str] / None -> text[]
        )

        if records:
            session.execute(UPSERT_REVIEW_RAG_SCORE, records)
        session.commit()


if __name__ == "__main__":
    main()
