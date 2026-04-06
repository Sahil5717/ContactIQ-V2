"""
ContactIQ — Transcript Analysis Engine
CR-FIX-TRANSCRIPT: Extracts insights from voice, chat, and email transcripts.
Produces sentiment analysis, bot handling metrics, silence patterns,
response time analysis, and SLA breach detection.
"""
import logging
from collections import defaultdict
from engines.data_loader import _resolve_path, read_xlsx_sheet

def run_transcript_analysis():
    """Main entry: analyze all transcript sources and return summary."""
    results = {
        'voice': _analyze_voice(),
        'chat': _analyze_chat(),
        'email': _analyze_email(),
        'summary': {},
    }
    
    # Build cross-channel summary
    all_sentiment = []
    for ch in ('voice', 'chat', 'email'):
        all_sentiment.extend(results[ch].get('sentiment_scores', []))
    
    avg_sentiment = sum(all_sentiment) / max(len(all_sentiment), 1) if all_sentiment else 0
    results['summary'] = {
        'totalInteractions': (results['voice'].get('totalSegments', 0) + 
                             results['chat'].get('totalMessages', 0) + 
                             results['email'].get('totalMessages', 0)),
        'avgSentiment': round(avg_sentiment, 3),
        'sentimentLabel': 'Positive' if avg_sentiment > 0.3 else 'Neutral' if avg_sentiment > -0.1 else 'Negative',
        'botHandlingRate': results['chat'].get('botHandlingRate', 0),
        'avgSilencePct': results['voice'].get('avgSilencePct', 0),
        'emailSLABreachRate': results['email'].get('slaBreachRate', 0),
        'source': 'interaction_transcripts.xlsx',
    }
    
    return results


def _analyze_voice():
    """Analyze voice transcripts for sentiment, silence, and quality patterns."""
    import os
    path = _resolve_path('raw/interaction_transcripts.xlsx')
    if not os.path.exists(path):
        return {'totalSegments': 0}
    try:
        rows = read_xlsx_sheet(path, 'Voice_Transcripts')
    except Exception:
        return {'totalSegments': 0}
    
    if not rows:
        return {'totalSegments': 0}
    
    # Aggregate by interaction
    interactions = defaultdict(lambda: {
        'segments': 0, 'duration': 0, 'silence': 0, 'sentiments': [],
        'queue': '', 'crosstalk': 0, 'intents': [],
    })
    
    sentiment_scores = []
    queue_sentiment = defaultdict(list)
    
    for r in rows:
        iid = r.get('Interaction_ID', '')
        ix = interactions[iid]
        ix['segments'] += 1
        ix['duration'] = float(r.get('Duration_Sec', 0) or 0)
        ix['queue'] = r.get('Queue_Name', '')
        
        sent = r.get('Sentiment_Score')
        if sent is not None:
            try:
                sv = float(sent)
                ix['sentiments'].append(sv)
                sentiment_scores.append(sv)
                if ix['queue']:
                    queue_sentiment[ix['queue']].append(sv)
            except (ValueError, TypeError):
                pass
        
        silence = float(r.get('Silence_Duration_Sec', 0) or 0)
        ix['silence'] += silence
        
        if r.get('Crosstalk_Flag'):
            ix['crosstalk'] += 1
        
        intent = r.get('Intent_Detected')
        if intent:
            ix['intents'].append(str(intent).strip())
    
    # Compute metrics
    total_interactions = len(interactions)
    total_duration = sum(ix['duration'] for ix in interactions.values())
    total_silence = sum(ix['silence'] for ix in interactions.values())
    avg_silence_pct = (total_silence / max(total_duration, 1)) * 100
    
    # Silence > 15% of call is a quality flag
    high_silence = sum(1 for ix in interactions.values() 
                       if ix['duration'] > 0 and (ix['silence'] / ix['duration']) > 0.15)
    
    # Queue-level sentiment
    queue_summary = {}
    for q, scores in queue_sentiment.items():
        avg = sum(scores) / len(scores)
        queue_summary[q] = {
            'avgSentiment': round(avg, 3),
            'label': 'Positive' if avg > 0.3 else 'Neutral' if avg > -0.1 else 'Negative',
            'count': len(scores),
        }
    
    logging.info(f'[Transcripts] Voice: {total_interactions} interactions, '
                 f'avg sentiment {sum(sentiment_scores)/max(len(sentiment_scores),1):.2f}, '
                 f'silence {avg_silence_pct:.1f}%')
    
    return {
        'totalSegments': len(rows),
        'totalInteractions': total_interactions,
        'avgSilencePct': round(avg_silence_pct, 1),
        'highSilenceInteractions': high_silence,
        'highSilencePct': round(high_silence / max(total_interactions, 1) * 100, 1),
        'sentiment_scores': sentiment_scores,
        'queueSentiment': queue_summary,
    }


def _analyze_chat():
    """Analyze chat transcripts for bot handling, response times, and sentiment."""
    import os
    path = _resolve_path('raw/interaction_transcripts.xlsx')
    if not os.path.exists(path):
        return {'totalMessages': 0}
    try:
        rows = read_xlsx_sheet(path, 'Chat_Transcripts')
    except Exception:
        return {'totalMessages': 0}
    
    if not rows:
        return {'totalMessages': 0}
    
    conversations = defaultdict(lambda: {
        'messages': 0, 'bot_handled': False, 'bot_confidence': 0,
        'response_times': [], 'sentiments': [], 'queue': '',
    })
    
    sentiment_scores = []
    
    for r in rows:
        cid = r.get('Conversation_ID', '')
        cx = conversations[cid]
        cx['messages'] += 1
        cx['queue'] = r.get('Queue_Name', '')
        
        if r.get('Bot_Handled'):
            bot_val = str(r.get('Bot_Handled', '')).strip().lower()
            if bot_val in ('true', 'yes', '1'):
                cx['bot_handled'] = True
                bc = r.get('Bot_Confidence')
                if bc is not None:
                    try:
                        cx['bot_confidence'] = max(cx['bot_confidence'], float(bc))
                    except (ValueError, TypeError):
                        pass
        
        rt = r.get('Response_Time_Sec')
        if rt is not None:
            try:
                cx['response_times'].append(float(rt))
            except (ValueError, TypeError):
                pass
        
        sent = r.get('Sentiment_Score')
        if sent is not None:
            try:
                sv = float(sent)
                cx['sentiments'].append(sv)
                sentiment_scores.append(sv)
            except (ValueError, TypeError):
                pass
    
    total_convs = len(conversations)
    bot_handled = sum(1 for cx in conversations.values() if cx['bot_handled'])
    bot_rate = bot_handled / max(total_convs, 1)
    
    all_response_times = []
    for cx in conversations.values():
        all_response_times.extend(cx['response_times'])
    avg_response_time = sum(all_response_times) / max(len(all_response_times), 1) if all_response_times else 0
    
    logging.info(f'[Transcripts] Chat: {total_convs} conversations, '
                 f'bot handling {bot_rate:.0%}, avg response {avg_response_time:.0f}s')
    
    return {
        'totalMessages': len(rows),
        'totalConversations': total_convs,
        'botHandled': bot_handled,
        'botHandlingRate': round(bot_rate, 3),
        'avgResponseTimeSec': round(avg_response_time, 1),
        'sentiment_scores': sentiment_scores,
    }


def _analyze_email():
    """Analyze email threads for SLA breaches, response times, and sentiment."""
    import os
    path = _resolve_path('raw/interaction_transcripts.xlsx')
    if not os.path.exists(path):
        return {'totalMessages': 0}
    try:
        rows = read_xlsx_sheet(path, 'Email_Threads')
    except Exception:
        return {'totalMessages': 0}
    
    if not rows:
        return {'totalMessages': 0}
    
    threads = defaultdict(lambda: {
        'messages': 0, 'sla_breached': False, 'response_hours': [],
        'sentiments': [], 'priority': '',
    })
    
    sentiment_scores = []
    
    for r in rows:
        tid = r.get('Thread_ID', '')
        tx = threads[tid]
        tx['messages'] += 1
        
        if str(r.get('SLA_Breach', '')).strip().lower() in ('true', 'yes', '1'):
            tx['sla_breached'] = True
        
        rh = r.get('Response_Time_Hours')
        if rh is not None:
            try:
                tx['response_hours'].append(float(rh))
            except (ValueError, TypeError):
                pass
        
        prio = r.get('Priority_Detected')
        if prio:
            tx['priority'] = str(prio).strip()
        
        sent = r.get('Sentiment_Score')
        if sent is not None:
            try:
                sv = float(sent)
                tx['sentiments'].append(sv)
                sentiment_scores.append(sv)
            except (ValueError, TypeError):
                pass
    
    total_threads = len(threads)
    sla_breached = sum(1 for tx in threads.values() if tx['sla_breached'])
    sla_breach_rate = sla_breached / max(total_threads, 1)
    
    all_response_hours = []
    for tx in threads.values():
        all_response_hours.extend(tx['response_hours'])
    avg_response_hours = sum(all_response_hours) / max(len(all_response_hours), 1) if all_response_hours else 0
    
    logging.info(f'[Transcripts] Email: {total_threads} threads, '
                 f'SLA breach {sla_breach_rate:.0%}, avg response {avg_response_hours:.1f}hrs')
    
    return {
        'totalMessages': len(rows),
        'totalThreads': total_threads,
        'slaBreached': sla_breached,
        'slaBreachRate': round(sla_breach_rate, 3),
        'avgResponseHours': round(avg_response_hours, 1),
        'sentiment_scores': sentiment_scores,
    }
