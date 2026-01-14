#!/usr/bin/env python3
"""
Unicode Sanitizer Mapper for Singer/Meltano pipelines.

This script reads Singer messages from stdin, sanitizes all string fields
to remove invalid Unicode surrogates, and outputs clean data to stdout.

Usage:
    tap-amplitude | python sanitize_unicode.py | target-postgres
"""

import sys
import json

# Force UTF-8 encoding on Windows
if sys.platform == 'win32':
    sys.stdin.reconfigure(encoding='utf-8', errors='replace')
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')


def sanitize_value(value):
    """
    Recursively sanitize a value, replacing invalid Unicode surrogates.
    
    Args:
        value: Any JSON-serializable value
        
    Returns:
        Sanitized value with invalid Unicode characters replaced with '?'
    """
    if isinstance(value, str):
        # Encode to UTF-8 with 'replace' error handling, then decode back
        # This replaces invalid surrogates with the replacement character (U+FFFD)
        try:
            return value.encode('utf-8', errors='replace').decode('utf-8')
        except Exception:
            # If encoding still fails, replace surrogates character by character
            return ''.join(
                char if ord(char) < 0xD800 or ord(char) > 0xDFFF else '?'
                for char in value
            )
    elif isinstance(value, dict):
        return {k: sanitize_value(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [sanitize_value(item) for item in value]
    else:
        return value


def process_message(message: dict) -> dict:
    """
    Process a Singer message and sanitize any string fields.
    
    Args:
        message: A parsed Singer message (SCHEMA, RECORD, STATE, etc.)
        
    Returns:
        Sanitized message
    """
    msg_type = message.get('type')
    
    if msg_type == 'RECORD':
        # Sanitize the record data
        message['record'] = sanitize_value(message.get('record', {}))
    elif msg_type == 'SCHEMA':
        # Schemas typically don't have encoding issues, but sanitize just in case
        pass
    elif msg_type == 'STATE':
        # State messages might have strings, sanitize them
        if 'value' in message:
            message['value'] = sanitize_value(message['value'])
    
    return message


def main():
    """Main entry point - read from stdin, sanitize, write to stdout."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        
        try:
            message = json.loads(line)
            sanitized = process_message(message)
            print(json.dumps(sanitized, ensure_ascii=False))
            sys.stdout.flush()
        except json.JSONDecodeError as e:
            # If we can't parse the line as JSON, pass it through
            print(line, file=sys.stderr)
            sys.stderr.flush()
        except Exception as e:
            # Log errors but try to continue
            print(f"Error processing line: {e}", file=sys.stderr)
            print(line)  # Pass through the original line
            sys.stdout.flush()


if __name__ == '__main__':
    main()
