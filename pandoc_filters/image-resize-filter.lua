-- A Pandoc Lua filter to add support for Obsidian-style image resizing.
--
-- Syntax: ![alt-text|widthxheight](path/to/image.png)
--         ![alt-text|width](path/to/image.png)
--
-- The filter parses the caption of an image. If it finds a pipe
-- symbol (|), it treats the text after the pipe as a width/height
-- specification. The width can also be a percentage.

-- This function will be called for every Image element in the document.
function Image(img)
  local potential_scale_str
  local new_caption_text
  -- The caption is a list of inline elements. We stringify it to get plain text.
  local caption_text = pandoc.utils.stringify(img.caption)
  if img.title == "wikilink" then
    potential_scale_str = caption_text
    new_caption_text = img.src
  else

    -- Check if the caption contains our resize delimiter '|'.
    local pipe_pos = caption_text:find('|', 1, true)
    if not pipe_pos then
      return nil -- Return nil to indicate no change was made.
    end
    -- Split the string into the actual caption and the dimension string.
    potential_scale_str = caption_text:sub(pipe_pos + 1)
    new_caption_text = caption_text:sub(1, pipe_pos - 1):match("^%s*(.-)%s*$")
  end


  if not potential_scale_str then
    return nil
  end

  local dim_string = potential_scale_str:match("^%s*(.-)%s*$")
  if not dim_string then
    return nil
  end

  -- Use string.match to parse the dimension string.
  -- It captures width and an optional height.
  local width, height = dim_string:match("^(%d+)[xX](%d+)$")

  -- If the widthxheight pattern doesn't match, try matching only the width.
  if not width then
    width = dim_string:match("^(%d+)$")
  end

  -- If we successfully found a width, we can modify the image element.
  if width then
    -- Update the image's attributes.
    img.attributes.width = width
    if height then
      img.attributes.height = height
    end

    -- Update the caption to remove the dimension part.
    -- pandoc.Str creates a new string element.
    if new_caption_text then
      img.caption = { pandoc.Str(new_caption_text) }
    end

    -- Return the modified image element.
    return img
  end

  -- If no valid dimension string was found after the pipe,
  -- we return the image unmodified.
  return nil
end
